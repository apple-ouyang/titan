/*
 * @Author: Wang Haitao
 * @Date: 2022-03-30 15:11:47
 * @LastEditTime: 2022-04-01 19:29:24
 * @LastEditors: Wang Haitao
 * @FilePath: /titan/src/delta_compression_test.cc
 * @Description: Github:https://github.com/apple-ouyang
 *  Gitee:https://gitee.com/apple-ouyang
 */

#include "blob_gc_job.h"
#include "blob_gc_picker.h"
#include "db_impl.h"
#include "delta_compression.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"
#include "titan/options.h"
#include "utilities/transactions/write_prepared_txn_db.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/filesystem/operations.hpp>
#include <cstdint>
#include <gtest/gtest.h>
#include <string>

namespace rocksdb {
namespace titandb {

namespace fs = boost::filesystem;
using namespace fs;
using namespace std;

enum DataSetType : uint8_t {
  kWikipedia,
  kEnronMail,
  kStackOverFlow,
  kStackOverFlowComment
};

const path data_path = "/home/wht/tao-db/test-titan/dataset/DataSet/";
const path wiki_directory = data_path / "wikipedia/article";
const path enron_email_directory = data_path / "enronMail";
const path stack_overflow_directory = data_path / "stackExchange";
const path stack_overflow_comment_file = data_path / "Comments.xml";

string exec(const char *cmd) {
  array<char, 128> buffer;
  string result;
  unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
  if (!pipe) {
    throw runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

size_t CountWikipediaHtmls(void) {
  string cmd = "find " + wiki_directory.string() + " -name '*.html' | wc -l";
  string res = exec(cmd.c_str());
  size_t size;
  sscanf(res.c_str(), "%zu", &size);
  return size;
}

size_t CountEnronEmails(void) {
  string cmd = "find " + enron_email_directory.string() + " | wc -l";
  string res = exec(cmd.c_str());
  size_t size;
  sscanf(res.c_str(), "%zu", &size);
  return size;
}

size_t CountStackOverFlowXmlFiles() {
  string cmd = "find " + stack_overflow_directory.string() + " | wc -l";
  string res = exec(cmd.c_str());
  size_t size;
  sscanf(res.c_str(), "%zu", &size);
  return size;
}

size_t CountLinesOfStackOverFlowComment() {
  string cmd = "wc -l " + stack_overflow_comment_file.string();
  string res = exec(cmd.c_str());
  size_t size;
  sscanf(res.c_str(), "%zu", &size);
  return size;
}

struct HumanReadable {
  uintmax_t size{};

private:
  friend ostream &operator<<(ostream &os, HumanReadable hr) {
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
  string dbname_;
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
    expected_percentage_ = 100;
  }

  ~DeltaCompressionTest() { Close(); }

  weak_ptr<BlobStorage> GetBlobStorage(uint32_t cf_id) {
    MutexLock l(mutex_);
    return blob_file_set_->GetBlobStorage(cf_id);
  }

  void ClearDir() {
    vector<string> filenames;
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
  void RunGC() {
    printf("Start garbage collection!\n");
    MutexLock l(mutex_);
    Status s;
    auto *cfh = base_db_->DefaultColumnFamily();

    // Build BlobGC
    TitanDBOptions db_options = options_;
    TitanCFOptions cf_options = options_;
    LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, db_options.info_log.get());

    uint64_t gc_processed_records = 0;
    uint64_t delta_compressed_records_num = 0;
    uint64_t delta_compressed_records_original_size = 0;
    uint64_t delta_compressed_records_delta_size = 0;
    unique_ptr<BlobGC> blob_gc;
    do {
      {
        shared_ptr<BlobGCPicker> blob_gc_picker =
            make_shared<BasicBlobGCPicker>(db_options, cf_options, nullptr);
        blob_gc = blob_gc_picker->PickBlobGC(
            blob_file_set_->GetBlobStorage(cfh->GetID()).lock().get());
      }

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
          ASSERT_OK(s);
          mutex_->Lock();
        }

        if (s.ok()) {
          s = blob_gc_job.Finish();
          ASSERT_OK(s);
        }
        blob_gc->ReleaseGcFiles();
        gc_processed_records += blob_gc_job.metrics_.gc_num_processed_records;
        delta_compressed_records_original_size +=
            blob_gc_job.metrics_.gc_before_delta_compressed_size;
        delta_compressed_records_delta_size +=
            blob_gc_job.metrics_.gc_after_delta_compressed_size;
        delta_compressed_records_num +=
            blob_gc_job.metrics_.gc_delta_compressed_record;
      }

      mutex_->Unlock();
      tdb_->PurgeObsoleteFiles();
      mutex_->Lock();
    } while (blob_gc != nullptr && blob_gc->trigger_next() &&
             gc_processed_records < total_records_);
    DeltaCompressType type = options_.blob_file_delta_compression;
    std::string delta_compression_str = "unknown";
    for (auto &delta_compression_type :
         TitanOptionsHelper::delta_compression_type_string_map) {
      if (delta_compression_type.second == type) {
        delta_compression_str = delta_compression_type.first;
        break;
      }
    }
    printf("\n----------    garbage collection complete!    ----------\n");
    printf("gc process %zu records\n", gc_processed_records);
    printf("Use %s to delta compress %zu similar records\n",
           delta_compression_str.c_str(), delta_compressed_records_num);
    printf("%zu bytes data are compressed to %zu byte\n",
           delta_compressed_records_original_size,
           delta_compressed_records_delta_size);
    printf("compression ratio is %.2f\n",
           (double)delta_compressed_records_original_size /
               delta_compressed_records_delta_size);
  }

  void PutDataPrepare(const DataSetType type) {
    printf("Scaning the number of files that can be Put into the "
           "database...\n");
    printf("Please wait, this may takes a few minutes...\n");
    switch (type) {
    case kWikipedia: {
      to_be_read_ = CountWikipediaHtmls();
      data_directory_ = wiki_directory;
      break;
    }
    case kEnronMail: {
      to_be_read_ = CountEnronEmails();
      data_directory_ = enron_email_directory;
      break;
    }
    case kStackOverFlow: {
      to_be_read_ = CountStackOverFlowXmlFiles();
      data_directory_ = stack_overflow_directory;
      break;
    }
    case kStackOverFlowComment: {
      to_be_read_ = CountLinesOfStackOverFlowComment();
      data_directory_ = stack_overflow_comment_file;
      break;
    }
    default:
      FAIL() << "wrong data set type!\n";
    }
    printf("%zu files can be put into the database\n", to_be_read_);
    printf("Data set path = %s\n", data_directory_.c_str());
  }

  void Put(const string &key, const string &value) {
    Status s = db_->Put(WriteOptions(), key, value);
    ASSERT_TRUE(s.ok());
    ++total_records_;
    put_key_value_size_.size += key.size() + value.size();
  }

  void PrintFinishInfo() {
    size_t max_similar = feature_index_table.GetMaxNumberOfSiimlarRecords();
    cout << '\n'
         << total_records_ << " records have been put into titan databse!"
         << endl;
    cout << put_key_value_size_ << " of keys and values have been put" << endl;
    cout << "Database is located in: " << dbname_ << endl;
    printf("%6zu is the max number of similar records that can be delta "
           "compressed in this database\n",
           max_similar);
    printf("%.2f percent records can be delta compressed\n",
           (double)max_similar / total_records_ * 100);
  }

  // expected_percentage_ range:[1-100]
  // once write percentage > expected percentage return true;
  bool IsFinishPut() {
    has_been_read_++;
    completion_percentage_ = 100 * has_been_read_ / to_be_read_;
    if (completion_percentage_ - last_completion_ >= 1) {
      printf("\rExpect Put %2zu%%, complete %2zu%%", expected_percentage_,
             completion_percentage_);
      fflush(stdout);
      last_completion_ = completion_percentage_;
    }
    if (completion_percentage_ >= expected_percentage_) {
      printf("\rExpect Put %2zu%%, complete %2zu%%\n", expected_percentage_,
             completion_percentage_);
      return true;
    }
    return false;
  }

  void ParseWikipediaHtml(path path, string &key, string &value) {
    fs::ifstream fin(path);
    stringstream buffer;
    // key: file name
    // key example: Category~Cabarets_in_Paris_7ad0.html
    // value: html file content
    buffer << fin.rdbuf();
    string content = buffer.str();
    string file_name = path.filename().string();
    key = move(file_name);
    value = move(content);
  }

  void ParseEnronMail(path path, string &key, string &value) {
    fs::ifstream fin(path);
    stringstream buffer;
    // key: first line
    // key example: Message-ID: <18782981.1075855378110.JavaMail.evans@thyme>
    // value: remaining email lines
    string first_line;
    getline(fin, first_line);
    buffer << fin.rdbuf();
    string remain_lines = buffer.str();
    key = move(first_line);
    value = move(remain_lines);
  }

  void ReadFilesUnderDirectoryThenPut(const DataSetType type) {
    PutDataPrepare(type);
    for (recursive_directory_iterator f(data_directory_), file_end;
         f != file_end; ++f) {
      if (!is_directory(f->path())) {
        string key, value;
        if (type == kWikipedia) {
          ParseWikipediaHtml(f->path(), key, value);
        } else if (type == kEnronMail) {
          ParseEnronMail(f->path(), key, value);
        } else {
          FAIL() << "wrong data set type!\n";
        }
        Put(key, value);
      }
      if (IsFinishPut())
        break;
    }
  }

  inline void PutWikipediaData() {
    ReadFilesUnderDirectoryThenPut(kWikipedia);
    PrintFinishInfo();
  }
  inline void PutEnronMailData() {
    ReadFilesUnderDirectoryThenPut(kEnronMail);
    PrintFinishInfo();
  }

  void PutStackOverFlowData() {
    PutDataPrepare(kStackOverFlow);
    for (fs::recursive_directory_iterator file(stack_overflow_directory),
         file_end;
         file != file_end; ++file) {
      if (!is_directory(file->path())) {
        fs::ifstream fin(file->path());
        string line;
        while (getline(fin, line)) {
          if (line.size() < 3)
            continue;
          // find record start with <row
          size_t found = line.find("<row");
          if (found == string::npos) {
            continue;
          }

          size_t start = line.rfind(" Id=");
          //<row  ...  Id="12345" ... ></row>
          // start:    ^
          ASSERT_FALSE(start == string::npos)
              << line << "\ndoesn't find Id=\n\n";

          start += 5;
          //<row  ... Id="12345" ... ></row>
          // start:        ^

          size_t end = line.find('"', start);
          //<row  ... Id="12345" ... ></row>
          // end:               ^

          ASSERT_FALSE(end == string::npos)
              << line << "\ndoesn't find right quotation!\n\n";

          // key = Id = "12345"
          string key = line.substr(start, end - start);
          string &value = line;
          Put(key, value);
        }
        if (IsFinishPut())
          break;
      }
    }
    PrintFinishInfo();
  }

  void PutStackOverFlowCommentData() {
    PutDataPrepare(kStackOverFlowComment);
    fs::ifstream fin(stack_overflow_comment_file);
    string line;
    const int kIdStartPosition = 11;
    while (getline(fin, line)) {
      // every record has this pattern:
      //  <row Id="12345" .../>
      // 01234567890
      // so we can find the Id through finding the right quotation after the
      // left quotation

      // some other lines, like start and end of the Comment.xml is not started
      // as "  <row"
      if (line.substr(0, 6) != "  <row") {
        // cout << line << '\n';
        // cout << "doesn't find <row" << "\n\n";
        continue;
      }

      // make sure <row follows the Id=
      ASSERT_STREQ(line.substr(7, 3).c_str(), "Id=") << "doesn't find Id=\n\n";

      // POS_ID_START=11, means the first number of the Id
      size_t pos_id_end = line.find('"', kIdStartPosition);
      ASSERT_NE(pos_id_end, string::npos)
          << line << "\ncan't find right quotation! \n\n";

      string key = line.substr(kIdStartPosition, pos_id_end - kIdStartPosition);
      string &value = line;
      Put(key, value);
      if (IsFinishPut())
        break;
    }
    PrintFinishInfo();
    ;
  }

  void TestWikipedia() {
    NewDB();
    PutWikipediaData();
    Flush();
    // RunGC();
  }

  void TestEnronMail() {
    NewDB();
    PutEnronMailData();
    Flush();
    // RunGC();
  }

  void TestStackOverFlow() {
    NewDB();
    PutStackOverFlowData();
    Flush();
    // RunGC();
  }

  void TestStackOverFlowComment() {
    NewDB();
    PutStackOverFlowCommentData();
    Flush();
    // RunGC();
  }

private:
  size_t to_be_read_;
  size_t has_been_read_ = 0;
  size_t total_records_ = 0;
  size_t completion_percentage_ = 0;
  size_t last_completion_ = 0;
  // expected_percentage_ range:[1-100]
  // once write data process percentage > expected percentage, write will stop
  size_t expected_percentage_;
  struct HumanReadable put_key_value_size_;
  path data_directory_;
};

TEST_P(DeltaCompressionTest, Wikipedia) { TestWikipedia(); }

TEST_P(DeltaCompressionTest, EnronMail) { TestEnronMail(); }

TEST_P(DeltaCompressionTest, StackOverFlow) { TestStackOverFlow(); }

TEST_P(DeltaCompressionTest, StackOverFlowComment) {
  TestStackOverFlowComment();
}

// INSTANTIATE_TEST_CASE_P(DeltaCompressionTestParameterized,
// DeltaCompressionTest,
//                         ::testing::Values(kGDelta, kXDelta, kEDelta));

INSTANTIATE_TEST_CASE_P(DeltaCompressionTestParameterized, DeltaCompressionTest,
                        ::testing::Values(kGDelta));

} // namespace titandb
} // namespace rocksdb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
