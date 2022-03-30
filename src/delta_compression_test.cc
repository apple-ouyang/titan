/*
 * @Author: Wang Haitao
 * @Date: 2022-03-30 15:11:47
 * @LastEditTime: 2022-03-30 16:17:40
 * @LastEditors: Wang Haitao
 * @FilePath: /titan/src/delta_compression_test.cc
 * @Description: Github:https://github.com/apple-ouyang
 *  Gitee:https://gitee.com/apple-ouyang
 */

#include "blob_gc_job.h"
#include "blob_gc_picker.h"
#include "db_impl.h"
#include "delta_compression.h"
#include "test_util/testharness.h"
#include "titan/options.h"

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>
#include <string>

namespace rocksdb {
namespace titandb {

using namespace boost::filesystem;
using namespace std;

const path wiki_data_path =
    "/home/wht/tao-db/test-titan/dataset/DataSet/wikipedia/article";

std::string exec(const char *cmd) {
  std::array<char, 128> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

int get_total_htmls(void) {
  string cmd = "find " + wiki_data_path.string() + " -name '*.html' | wc -l";
  string res = exec(cmd.c_str());
  return stoi(res);
}

struct HumanReadable {
  uintmax_t size{};

private:
  friend std::ostream &operator<<(std::ostream &os, HumanReadable hr) {
    int i{};
    double mantissa = hr.size;
    for (; mantissa >= 1024.; mantissa /= 1024., ++i) {
    }
    mantissa = ceil(mantissa * 10.) / 10.;
    os << mantissa << "BKMGTPE"[i];
    return i == 0 ? os : os << "B (" << hr.size << ')';
  }
};

class DeltaCompressionTest : public testing::TestWithParam<DeltaCompressType> {
public:
  std::string dbname_;
  TitanDB *db_;
  DBImpl *base_db_;
  TitanDBImpl *tdb_;
  BlobFileSet *blob_file_set_;
  TitanOptions options_;
  port::Mutex *mutex_;

  DeltaCompressionTest() : dbname_(test::TmpDir()) {
    options_.dirname = dbname_ + "/titandb";
    options_.create_if_missing = true;
    options_.disable_background_gc = true;
    options_.min_blob_size = 0;
    options_.disable_auto_compactions = true;
    options_.env->CreateDirIfMissing(dbname_);
    options_.env->CreateDirIfMissing(options_.dirname);
    options_.blob_file_delta_compression = GetParam();

    // gc all blobs no mater How many garbage data they have
    options_.blob_file_discardable_ratio = -0.1;
  }

  ~DeltaCompressionTest() { Close(); }

  std::weak_ptr<BlobStorage> GetBlobStorage(uint32_t cf_id) {
    MutexLock l(mutex_);
    return blob_file_set_->GetBlobStorage(cf_id);
  }

  void ClearDir() {
    std::vector<std::string> filenames;
    options_.env->GetChildren(options_.dirname, &filenames);
    for (auto &fname : filenames) {
      if (fname != "." && fname != "..") {
        ASSERT_OK(options_.env->DeleteFile(options_.dirname + "/" + fname));
      }
    }
    options_.env->DeleteDir(options_.dirname);
    filenames.clear();
    options_.env->GetChildren(dbname_, &filenames);
    for (auto &fname : filenames) {
      if (fname != "." && fname != "..") {
        options_.env->DeleteFile(dbname_ + "/" + fname);
      }
    }
  }

  void NewDB() {
    ClearDir();
    Open();
  }

  void Open() {
    ASSERT_OK(TitanDB::Open(options_, dbname_, &db_));
    tdb_ = reinterpret_cast<TitanDBImpl *>(db_);
    blob_file_set_ = tdb_->blob_file_set_.get();
    mutex_ = &tdb_->mutex_;
    base_db_ = reinterpret_cast<DBImpl *>(tdb_->GetRootDB());
  }

  void Flush() {
    FlushOptions fopts;
    fopts.wait = true;
    ASSERT_OK(db_->Flush(fopts));
  }

  void Close() {
    if (!db_)
      return;
    ASSERT_OK(db_->Close());
    delete db_;
    db_ = nullptr;
  }

  // TODO: unifiy this and TitanDBImpl::TEST_StartGC
  void RunGC(bool expect_gc) {
    MutexLock l(mutex_);
    Status s;
    auto *cfh = base_db_->DefaultColumnFamily();

    // Build BlobGC
    TitanDBOptions db_options = options_;
    TitanCFOptions cf_options = options_;
    LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, db_options.info_log.get());

    std::unique_ptr<BlobGC> blob_gc;
    {
      std::shared_ptr<BlobGCPicker> blob_gc_picker =
          std::make_shared<BasicBlobGCPicker>(db_options, cf_options, nullptr);
      blob_gc = blob_gc_picker->PickBlobGC(
          blob_file_set_->GetBlobStorage(cfh->GetID()).lock().get());
    }

    ASSERT_TRUE((blob_gc != nullptr) == expect_gc);

    if (blob_gc) {
      blob_gc->SetColumnFamily(cfh);

      BlobGCJob blob_gc_job(blob_gc.get(), base_db_, tdb_, mutex_,
                            tdb_->db_options_, GetParam(), tdb_->env_,
                            EnvOptions(options_), tdb_->blob_manager_.get(),
                            blob_file_set_, &log_buffer, nullptr, nullptr);

      s = blob_gc_job.Prepare();
      ASSERT_OK(s);

      {
        mutex_->Unlock();
        s = blob_gc_job.Run();
        if (expect_gc) {
          ASSERT_OK(s);
        }
        mutex_->Lock();
      }

      if (s.ok()) {
        s = blob_gc_job.Finish();
        ASSERT_OK(s);
      }
      blob_gc->ReleaseGcFiles();
    }

    mutex_->Unlock();
    tdb_->PurgeObsoleteFiles();
    mutex_->Lock();
  }

  size_t WriteWikipediaData(double expected_write_percentage = 1) {
    std::cout << "please wait, this may takes a few minutes" << std::endl;
    int tot = get_total_htmls();
    double pre_per = 0;
    size_t cnt_htmls = 0;
    Status s;
    struct HumanReadable tot_size;
    for (recursive_directory_iterator f(wiki_data_path), end; f != end; ++f)
      if (!is_directory(f->path())) {
        string path = f->path().string();
        string filename = f->path().filename().string();
        // cout << "find file:" << path << endl;
        std::ifstream fin(path);
        std::stringstream buffer;
        buffer << fin.rdbuf();
        string content = buffer.str();

        s = db_->Put(WriteOptions(), filename, content);
        if (!s.ok())
          std::cerr << s.ToString() << std::endl;
        else {
          tot_size.size += filename.size() + content.size();
          ++cnt_htmls;
          double completion = (double)cnt_htmls / tot;
          if (completion - pre_per > 0.01) {
            std::cout << std::setprecision(2) << completion * 100 << "% done"
                      << std::endl;
            pre_per = completion;
          }
          if (completion > expected_write_percentage)
            break;
        }
      }
    std::cout << cnt_htmls << " htmls have been insert into titan databse! "
              << std::endl;
    std::cout << "The altogether size of keys and values are " << tot_size
              << std::endl;
    std::cout << "rocks database is located in: " << dbname_ << std::endl;
    return cnt_htmls;
  }

  void TestWikipediaData() {
    NewDB();
    WriteWikipediaData();
    Flush();
    RunGC(true);
  }
};

TEST_P(DeltaCompressionTest, WikipediaData) { TestWikipediaData(); }

INSTANTIATE_TEST_CASE_P(DeltaCompressionTestParameterized, DeltaCompressionTest,
                        ::testing::Values(kXDelta, kGDelta, kEDelta));

} // namespace titandb
} // namespace rocksdb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
