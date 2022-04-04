/**
 * @Author: Wang Haitao
 * @Date: 2021-12-24 10:10:27
 * @LastEditTime: 2021-12-24 11:35:18
 * @LastEditors: Wang Haitao
 * @FilePath: /titan/src/delta_compression.h
 * @Description: https://github.com/apple-ouyang
 */

#pragma once

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch_base.h"
#include "titan/options.h"
#include "util.h"
#include "util/xxhash.h"

#include <cstdint>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace rocksdb {

namespace titandb {

// Number of super features that each record will have.
const size_t kSuperFeatureNumber = 3;

// The Mask has 7 bits of 1's, so the sample rate is 1/(2^7)=1/128. It means the
// number of sampled chunks to generate feature will be 1/128 of the all sliding
// window chunks.
const uint64_t kSampleMask = 0x0000400303410000;

// const uint64_t kSampleMask =    0x0000000100000001;

typedef XXH64_hash_t super_feature_t;

// The super feature are used for similarity detection. The more of super
// features a record have, the bigger feature index table will be.
struct SuperFeatures {
  super_feature_t super_features[kSuperFeatureNumber];
};

using std::map;
using std::unordered_map;
using std::unordered_set;
using std::string;
using std::vector;

class FeatureIndexTable {
public:
  // generate the super features of the value
  // index the key-feature
  void Put(const Slice &key, const Slice &value);

  // Delete (key,3 super feature) pair and 3 ( super feature,key) pairs
  inline void Delete(const Slice &key) { Delete(key.ToString()); }

  void RangeDelete(const Slice &start, const Slice &end);

  Status Write(WriteBatch *updates);

  // Use key to find all similar records by searching the key-feature table.
  // After that, remove key from the key-feature table
  uint32_t GetSimilarRecordsKeys(const Slice &key,
                                 vector<string> &similar_keys);
                                 
  size_t GetMaxNumberOfSiimlarRecords();

private:
  unordered_map<super_feature_t, unordered_set<string>> feature_key_table;
  map<string, SuperFeatures> key_feature_table;

  void Delete(const string &key);

  void ExecuteDelete(const string &key, const SuperFeatures &super_features);

  bool GetSuperFeatures(const string &key, SuperFeatures *super_features);
};

class FeatureHandle : public WriteBatch::Handler, public FeatureIndexTable {
public:
  // using FeatureIndexTable::Put
  virtual void Put(const Slice &key, const Slice &value) override {
    FeatureIndexTable::Put(key, value);
  }

  virtual void Delete(const Slice &key) override {
    FeatureIndexTable::Delete(key);
  }
};

class FeatureSample {
public:
  /**
   * @description: Detect records similarity. Then we can use delta compression
   * to compress the similar values.
   * @param feature_num number of features to generate. should be multiply of
   * kSuperFeatureNumber.
   */
  FeatureSample(uint8_t features_num = 12);
  ~FeatureSample();

  SuperFeatures GenerateFeatures(const Slice &value);

private:
  /**
   * @summary: Use Odess method to calculate the features of a value. The
   * feature is used to detect similarity.
   * @description:  Use Gear hash to calculate the rolling hash of the values.
   * Use content defined method to sample some of chunks hash values. Use
   * different tramsformation methods to sample the hash value as the similarity
   * feature. If two value has a same feature, we consider they are similar.
   * @param &value the value of record.
   */
  void OdessResemblanceDetect(const Slice &value);

  /**
   * @description: Divide the features into kSuperFeatureNumber groups. Use
   * xxhash on each groups of feature to generate hash value as super feature.
   * @param super_features the generated super feature
   */
  void GroupFeatures(SuperFeatures *super_features);

  uint8_t features_num_;
  uint64_t *features_;
  uint64_t *transform_args_a_; // random numbers
  uint64_t *transform_args_b_;
};

// Returns true if:
// (1) the compression method is supported in this platform and
// (2) the compression rate is "good enough".
bool DeltaCompress(DeltaCompressType type, const Slice &input,
                   const Slice &base, std::string *output);

Status DeltaUncompress(DeltaCompressType type, const Slice &delta,
                       const Slice &base, OwnedSlice *output);

// 全局变量： TODO：暂时不知道放哪里
extern FeatureIndexTable feature_index_table;

} // namespace titandb
} // namespace rocksdb
