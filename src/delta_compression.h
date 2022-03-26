/**
 * @Author: Wang Haitao
 * @Date: 2021-12-24 10:10:27
 * @LastEditTime: 2021-12-24 11:35:18
 * @LastEditors: Wang Haitao
 * @FilePath: /titan/src/delta_compression.h
 * @Description: https://github.com/apple-ouyang
 */

#pragma once

#include "options/options_helper.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch_base.h"
#include "titan/options.h"
#include "util.h"
#include "util/xxhash.h"

#include <cstdint>
#include <forward_list>
#include <map>
#include <vector>

namespace rocksdb {

namespace titandb {

#define NUM_SUPER_FEATURE                                                      \
  3 // Number of super features that each record will have.

#define SAMPLE_MASK                                                            \
  0x0000400303410000 // It has 7 bits of 1's, so the sample rate is
                     // 1/(2^7)=1/128. It means the number of sampled chunks to
                     // generate feature will be 1/128 of the all sliding window
                     // chunks.

typedef XXH64_hash_t super_feature_t;

// The super feature are used for similarity detection. The more of super
// features a record have, the bigger feature index table will be.
struct SuperFeatures {
  super_feature_t super_features[NUM_SUPER_FEATURE];
};

using std::forward_list;
using std::map;
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

  // Find similar records' value by the key, put thoese similar records' keys in
  // the similar_keys. return the number of similar records.
  uint32_t FindKeysOfSimilarRecords(const Slice &key,
                                    vector<Slice> &similar_keys);

private:
  // TODO(haitao)除了链表还有什么高效的结构吗？
  map<super_feature_t, forward_list<string>> feature_key_table;
  map<string, SuperFeatures> key_feature_table;

  // return true if find the super features of the key
  inline bool GetSuperFeatures(const Slice &key, SuperFeatures *sfs = nullptr) {
    return GetSuperFeatures(key.ToString(), sfs);
  }

  // Delete (key, 3feature) pair
  void DeleteFeaturesOfAKey(const string &key, const SuperFeatures &sfs);

  void Delete(const string &key);

  void ExecuteDelete(const string &key, const SuperFeatures &sfs);

  bool GetSuperFeatures(const string &key, SuperFeatures *sfs = nullptr);
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
   * NUM_SUPER_FEATURE.
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
   * @description: Divide the features into NUM_SUPER_FEATURE groups. Use xxhash
   * on each groups of feature to generate hash value as super feature.
   * @param sfs the generated super feature
   */
  void GroupFeatures(SuperFeatures *sfs);

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
