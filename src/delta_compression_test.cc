#include "blob_gc_job.h"
#include "blob_gc_picker.h"
#include "db_impl.h"
#include "delta_compression.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "test_util/testharness.h"
#include "titan/options.h"
#include "utilities/transactions/write_prepared_txn_db.h"

#include <bits/types/struct_timespec.h>
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

struct HumanReadable {
  uintmax_t size_;
  HumanReadable(size_t size = 0) : size_(size){};
  std::string ToString(bool with_byte = true) {
    int magnitude = 0;
    double mantissa = size_;
    while (mantissa >= 1024) {
      mantissa /= 1024.;
      ++magnitude;
    }

    mantissa = ceil(mantissa * 10.) / 10.;

    stringstream ss;
    ss << fixed << setprecision(2) << mantissa;
    string res = ss.str();
    res += "BKMGTPE"[magnitude];
    if (magnitude > 0) {
      res += 'B';
      if (with_byte)
        res += '(' + to_string(size_) + ')';
    }

    return res;
  }

private:
  friend ostream &operator<<(ostream &os, HumanReadable hr) {
    return os << hr.ToString();
  }
};

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

size_t RunCmdReturnSize(string cmd) {
  string res = exec(cmd.c_str());
  size_t size;
  sscanf(res.c_str(), "%zu", &size);
  return size;
}

size_t GetDatabaseSize(string db_path) {
  string cmd = "du -s " + db_path;
  return RunCmdReturnSize(cmd) * 1024;
}

class DataReader {
public:
  DataReader(size_t expected_percentage = 100)
      : expected_percentage_(expected_percentage){};
  virtual ~DataReader(){};

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

  size_t CountWikipediaHtmls(void) {
    string cmd = "find " + wiki_directory.string() + " -name '*.html' | wc -l";
    return RunCmdReturnSize(cmd);
  }

  size_t CountEnronEmails(void) {
    string cmd = "find " + enron_email_directory.string() + " | wc -l";
    return RunCmdReturnSize(cmd);
  }

  size_t CountStackOverFlowXmlFiles() {
    string cmd = "find " + stack_overflow_directory.string() + " | wc -l";
    return RunCmdReturnSize(cmd);
  }

  size_t CountLinesOfStackOverFlowComment() {
    string cmd = "wc -l " + stack_overflow_comment_file.string();
    return RunCmdReturnSize(cmd);
  }

  void ReadDataPrepare(const DataSetType type) {
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

  virtual void GetSimilarRecords() = 0;

  void PrintFinishInfo() {
    printf("\n##################################################\n");
    cout << total_records_ << " records have been put into titan databse!\n";
    cout << put_key_value_size_ << " are the size of keys and values\n";
    printf("%zu (%.2f%%) is the number of similar records that can be delta "
           "compressed\n",
           max_similar_records_,
           (double)max_similar_records_ / total_records_ * 100);
    printf("##################################################\n\n");
    fflush(stdout);
  }

  void Finish() {
    GetSimilarRecords();
    PrintFinishInfo();
  }

  bool IsFinish() {
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

  virtual void ExecutePut(const string &key, const string &value) = 0;

  void Put(const string &key, const string &value) {
    ExecutePut(key, value);
    ++total_records_;
    put_key_value_size_.size_ += key.size() + value.size();
  }

  void ReadFilesUnderDirectoryThenPut(const DataSetType type) {
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
      if (IsFinish())
        break;
    }
  }

  void ReadParseStackOverFlowDataAndPut() {
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
      }
      // we count files as finish, not records this time.
      if (IsFinish())
        break;
    }
  }

  void ReadParseStackOverFlowCommentFileAndPut() {
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
      if (IsFinish())
        break;
    }
  }

  inline void PutWikipediaData() {
    ReadDataPrepare(kWikipedia);
    ReadFilesUnderDirectoryThenPut(kWikipedia);
    Finish();
  }
  inline void PutEnronMailData() {
    ReadDataPrepare(kEnronMail);
    ReadFilesUnderDirectoryThenPut(kEnronMail);
    Finish();
  }

  void PutStackOverFlowData() {
    ReadDataPrepare(kStackOverFlow);
    ReadParseStackOverFlowDataAndPut();
    Finish();
  }

  void PutStackOverFlowCommentData() {
    ReadDataPrepare(kStackOverFlowComment);
    ReadParseStackOverFlowCommentFileAndPut();
    Finish();
  }

  size_t to_be_read_;
  size_t has_been_read_ = 0;
  size_t total_records_ = 0;
  size_t completion_percentage_ = 0;
  size_t last_completion_ = 0;
  // expected_percentage_ range:[1-100]
  // once write data process percentage > expected percentage, write will stop
  size_t expected_percentage_;
  size_t max_similar_records_;
  struct HumanReadable put_key_value_size_;
  path data_directory_;
};

class DeltaCompressionTest : public testing::TestWithParam<DeltaCompressType>,
                             public DataReader {
public:
  string dbname_;
  TitanDB *db_;
  DBImpl *base_db_;
  TitanDBImpl *tdb_;
  BlobFileSet *blob_file_set_;
  TitanOptions options_;
  port::Mutex *mutex_;

  void Put(const string &key, const string &value) {
    Status s = db_->Put(WriteOptions(), key, value);
    ASSERT_TRUE(s.ok());
  }

  DeltaCompressionTest() : DataReader(100), dbname_(test::TmpDir()) {
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

  struct Statistics {
    uint64_t gc_number = 0;
    uint64_t compressed_number = 0;
    HumanReadable delta_before_size{};
    HumanReadable delta_after_size{};
    string method = "unknown";
    timespec compressed_time{};
    uint64_t fail_number = 0;
    HumanReadable database_before_size{};
    HumanReadable database_after_size{};

    void AddTimespec(timespec &time, const timespec &addtime) {
      time.tv_sec += addtime.tv_sec;
      time.tv_nsec += addtime.tv_nsec;
      if (time.tv_nsec > 1000000000) {
        time.tv_nsec -= 1000000000;
        time.tv_sec++;
      }
    }

    double TimespecToSeconds(const timespec &time) {
      return time.tv_sec + time.tv_nsec / 1000000000.;
    }

    void Update(const BlobGCJob::Metrics &metrics) {
      gc_number += metrics.gc_num_processed_records;
      delta_before_size.size_ += metrics.gc_before_delta_compressed_size;
      delta_after_size.size_ += metrics.gc_after_delta_compressed_size;
      compressed_number += metrics.gc_delta_compressed_record;
      fail_number += metrics.gc_delta_compressed_failed_number;
      AddTimespec(compressed_time, metrics.gc_delta_compressed_time);
    }

    void GetTypeString(DeltaCompressType type) {
      for (auto &delta_compression_type :
           TitanOptionsHelper::delta_compression_type_string_map) {
        if (delta_compression_type.second == type) {
          method = delta_compression_type.first;
          break;
        }
      }
    }

    void Print() {
      double delta_ratio =
          (double)delta_before_size.size_ / delta_after_size.size_;
      double database_ratio =
          (double)database_before_size.size_ / database_after_size.size_;
      double time = TimespecToSeconds(compressed_time);
      putchar('\n');
      printf(
          "| method  | compress fail | compress success | delta size | delta "
          "after size | delta compress ratio | compress time | database size | "
          "database after size | database compress ratio |\n");
      printf(
          "| ------- | ------------- | ---------------- | ---------- | "
          "---------------- | -------------------- | ------------- | "
          "------------- | ------------------- | ----------------------- |\n");
      printf("| %s\t | %zu\t | %zu\t | %s\t | %s\t | %.2f\t | "
             "%.2fs\t |  %s\t | %s\t | %.2f\t |\n",
             method.c_str(), fail_number, compressed_number,
             delta_before_size.ToString(false).c_str(),
             delta_after_size.ToString(false).c_str(), delta_ratio, time,
             database_before_size.ToString(false).c_str(),
             database_after_size.ToString(false).c_str(), database_ratio);
      fflush(stdout);
    }
  };

  bool IsKeepGC(uint64_t gc_processed_records) {
    double gc_ratio = (double)gc_processed_records / total_records_;
    printf("GC complete %.2f%%\n", gc_ratio * 100);
    fflush(stdout);
    return gc_ratio < 0.9;
  }

  // TODO unifiy this and TitanDBImpl::TEST_StartGC
  void RunGC() {
    Statistics statistics;
    statistics.database_before_size.size_ = GetDatabaseSize(dbname_);
    cout << "Start garbage collection!" << endl;
    MutexLock l(mutex_);
    Status s;
    auto *cfh = base_db_->DefaultColumnFamily();

    // Build BlobGC
    TitanDBOptions db_options = options_;
    TitanCFOptions cf_options = options_;
    LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, db_options.info_log.get());

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
        statistics.Update(blob_gc_job.metrics_);
      }

      mutex_->Unlock();
      tdb_->PurgeObsoleteFiles();
      mutex_->Lock();
    } while (blob_gc != nullptr && blob_gc->trigger_next() &&
             IsKeepGC(statistics.gc_number));
    // } while (blob_gc != nullptr && blob_gc->trigger_next());
    statistics.GetTypeString(options_.blob_file_delta_compression);
    statistics.database_after_size.size_ = GetDatabaseSize(dbname_);
    statistics.Print();
  }

  void ExecutePut(const string &key, const string &value) override {
    Status s = db_->Put(WriteOptions(), key, value);
    ASSERT_OK(s);
  }

  void GetSimilarRecords() override {
    max_similar_records_ = feature_index_table.CountAllSimilarRecords();
  }

  void TestWikipedia() {
    NewDB();
    PutWikipediaData();
    Flush();
    RunGC();
  }

  void TestEnronMail() {
    NewDB();
    PutEnronMailData();
    Flush();
    RunGC();
  }

  void TestStackOverFlow() {
    NewDB();
    PutStackOverFlowData();
    Flush();
    RunGC();
  }

  void TestStackOverFlowComment() {
    NewDB();
    PutStackOverFlowCommentData();
    Flush();
    RunGC();
  }
};

class ResemblenceDetectionTest
    : public testing::TestWithParam<tuple<super_feature_t, size_t, size_t>>,
      public DataReader {
public:
  ResemblenceDetectionTest()
      : DataReader(100),
        table_(get<0>(GetParam()), get<1>(GetParam()), get<2>(GetParam())){};

  void ExecutePut(const string &key, const string &value) override {
    table_.Put(key, value);
  }

  void GetSimilarRecords() override {
    max_similar_records_ = table_.CountAllSimilarRecords();
  }

  void TestWikipedia() { PutWikipediaData(); }

  void TestEnronMail() { PutEnronMailData(); }

  void TestStackOverFlow() { PutStackOverFlowData(); }

  void TestStackOverFlowComment() { PutStackOverFlowCommentData(); }

private:
  FeatureIndexTable table_;
};

// TEST_P(ResemblenceDetectionTest, Wikipedia) { TestWikipedia(); }
// TEST_P(ResemblenceDetectionTest, EnronMail) { TestEnronMail(); }
// TEST_P(ResemblenceDetectionTest, StackOverFlow) { TestStackOverFlow(); }
// TEST_P(ResemblenceDetectionTest, StackOverFlowComment) {
//   TestStackOverFlowComment();
// }

// TEST_P(DeltaCompressionTest, Wikipedia) { TestWikipedia(); }
TEST_P(DeltaCompressionTest, EnronMail) { TestEnronMail(); }
// TEST_P(DeltaCompressionTest, StackOverFlow) { TestStackOverFlow(); }
// TEST_P(DeltaCompressionTest, StackOverFlowComment) {
//   TestStackOverFlowComment();
// }

typedef tuple<feature_t, size_t, size_t> TableParameters;

// INSTANTIATE_TEST_CASE_P(
//     ResemblenceDetectionTestParameterized, ResemblenceDetectionTest,
//     ::testing::Values(TableParameters(k1_128RatioMask, 12, 3),
//                       TableParameters(k1_4RatioMask, 12, 3)
//                       TableParameters(k1_4RatioMask, 12, 12)
//                       ));

INSTANTIATE_TEST_CASE_P(DeltaCompressionTestParameterized, DeltaCompressionTest,
                        ::testing::Values(kGDelta, kXDelta, kEDelta));

// INSTANTIATE_TEST_CASE_P(DeltaCompressionTestParameterized,
// DeltaCompressionTest,
//                         ::testing::Values(kGDelta));

} // namespace titandb
} // namespace rocksdb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
