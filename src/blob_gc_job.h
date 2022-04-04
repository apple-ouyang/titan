#pragma once

#include "blob_file_builder.h"
#include "blob_file_iterator.h"
#include "blob_file_manager.h"
#include "blob_file_set.h"
#include "blob_gc.h"
#include "db/db_impl/db_impl.h"
#include "db_impl.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "titan/options.h"
#include "titan_stats.h"
#include "version_edit.h"
#include <cstdint>
#include <string>
#include <vector>

namespace rocksdb {
namespace titandb {

using std::string;
using std::vector;

class BlobGCJob {
public:
  BlobGCJob(BlobGC *blob_gc, DB *db, TitanDBImpl *titan, port::Mutex *mutex,
            const TitanDBOptions &titan_db_options, bool gc_merge_rewrite,
            Env *env, const EnvOptions &env_options,
            BlobFileManager *blob_file_manager, BlobFileSet *blob_file_set,
            LogBuffer *log_buffer, std::atomic_bool *shuting_down,
            TitanStats *stats);

  // No copying allowed
  BlobGCJob(const BlobGCJob &) = delete;
  void operator=(const BlobGCJob &) = delete;

  ~BlobGCJob();

  // REQUIRE: mutex held
  Status Prepare();
  // REQUIRE: mutex not held
  Status Run();
  // REQUIRE: mutex held
  Status Finish();

private:
  class GarbageCollectionWriteCallback;
  friend class BlobGCJobTest;
  friend class DeltaCompressionTest;

  void UpdateInternalOpStats();

  BlobGC *blob_gc_;
  DB *base_db_;
  DBImpl *base_db_impl_;
  TitanDBImpl *titan_db_impl_;
  port::Mutex *mutex_;
  TitanDBOptions db_options_;
  const bool gc_merge_rewrite_;
  Env *env_;
  EnvOptions env_options_;
  BlobFileManager *blob_file_manager_;
  BlobFileSet *blob_file_set_;
  LogBuffer *log_buffer_{nullptr};

  std::vector<std::pair<std::unique_ptr<BlobFileHandle>,
                        std::unique_ptr<BlobFileBuilder>>>
      blob_file_builders_;
  std::vector<std::pair<WriteBatch, GarbageCollectionWriteCallback>>
      rewrite_batches_;
  std::vector<std::pair<WriteBatch, uint64_t /*blob_record_size*/>>
      rewrite_batches_without_callback_;

  std::atomic_bool *shuting_down_{nullptr};

  TitanStats *stats_;

  struct {
    uint64_t gc_bytes_read = 0;
    uint64_t gc_bytes_written = 0;
    uint64_t gc_num_keys_overwritten = 0;
    uint64_t gc_bytes_overwritten = 0;
    uint64_t gc_num_keys_relocated = 0;
    uint64_t gc_bytes_relocated = 0;
    uint64_t gc_num_new_files = 0;
    uint64_t gc_num_files = 0;
    uint64_t gc_small_file = 0;
    uint64_t gc_discardable = 0;
    uint64_t gc_sample = 0;
    uint64_t gc_sampling_micros = 0;
    uint64_t gc_read_lsm_micros = 0;
    uint64_t gc_update_lsm_micros = 0;
    uint64_t gc_num_processed_records = 0;
    uint64_t gc_delta_compressed_record = 0;
    uint64_t gc_before_delta_compressed_size = 0;
    uint64_t gc_after_delta_compressed_size = 0;
  } metrics_;

  uint64_t prev_bytes_read_ = 0;
  uint64_t prev_bytes_written_ = 0;
  uint64_t io_bytes_read_ = 0;
  uint64_t io_bytes_written_ = 0;

  Status DoRunGC();
  void BatchWriteNewIndices(BlobFileBuilder::OutContexts &contexts, Status *s);
  Status BuildIterator(std::unique_ptr<BlobFileMergeIterator> *result);
  Status DiscardEntry(const Slice &key, const BlobIndex &blob_index,
                      bool *discardable);
  inline void DiscardBaseEntry(const BlobType type, const uint16_t base_ref,
                               bool *discardable);
  Status InstallOutputBlobFiles();
  Status RewriteValidKeyToLSM();
  Status DeleteInputBlobFiles();

  bool IsShutingDown();
  Status DeltaCompressRecords(const Slice &base,
                              const vector<string> &similar_keys,
                              vector<string> &deltas_keys,
                              vector<string> &deltas_value,
                              vector<BlobIndex> &delta_indexes);

  // Find all delta records beneth the base record
  // Save thoese valid delta records infomation
  Status IterateDeltasUnderBase(std::unique_ptr<BlobFileMergeIterator> &gc_iter,
                                const size_t kDeltasNumber,
                                vector<string> &keys, vector<string> &values,
                                vector<BlobIndex> &indexes);
  // There is two source of deltas
  // 1. the iterator get a kBlobRecord/kBaseRecord X, and the delta compression
  // is on. so we find all the similar records of X, and delta compress them
  // based on X. We call X baes and thoese compressed similar records as deltas.
  // Then we write the base and the follwing deltas to the blob.
  // 2. the iterator get a kBaseRecord X, meaning there should be some deltas
  // based on X. So we read thoese deltas just below the base, dicard those
  // invalid deltas, and write thos valid deltas below the base
  Status WriteDeltas(const BlobIndex &new_base_index,
                     const vector<string> &keys, const vector<string> &values,
                     vector<BlobIndex> &indexes, uint64_t write_file_number,
                     const std::unique_ptr<BlobFileBuilder> &blob_file_builder);
};

}  // namespace titandb
}  // namespace rocksdb
