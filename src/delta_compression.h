/**
 * @......................................&&.........................
 * @....................................&&&..........................
 * @.................................&&&&............................
 * @...............................&&&&..............................
 * @.............................&&&&&&..............................
 * @...........................&&&&&&....&&&..&&&&&&&&&&&&&&&........
 * @..................&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&..............
 * @................&...&&&&&&&&&&&&&&&&&&&&&&&&&&&&.................
 * @.......................&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&.........
 * @...................&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&...............
 * @..................&&&   &&&&&&&&&&&&&&&&&&&&&&&&&&&&&............
 * @...............&&&&&@  &&&&&&&&&&..&&&&&&&&&&&&&&&&&&&...........
 * @..............&&&&&&&&&&&&&&&.&&....&&&&&&&&&&&&&..&&&&&.........
 * @..........&&&&&&&&&&&&&&&&&&...&.....&&&&&&&&&&&&&...&&&&........
 * @........&&&&&&&&&&&&&&&&&&&.........&&&&&&&&&&&&&&&....&&&.......
 * @.......&&&&&&&&.....................&&&&&&&&&&&&&&&&.....&&......
 * @........&&&&&.....................&&&&&&&&&&&&&&&&&&.............
 * @..........&...................&&&&&&&&&&&&&&&&&&&&&&&............
 * @................&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&............
 * @..................&&&&&&&&&&&&&&&&&&&&&&&&&&&&..&&&&&............
 * @..............&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&....&&&&&............
 * @...........&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&......&&&&............
 * @.........&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&.........&&&&............
 * @.......&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&...........&&&&............
 * @......&&&&&&&&&&&&&&&&&&&...&&&&&&...............&&&.............
 * @.....&&&&&&&&&&&&&&&&............................&&..............
 * @....&&&&&&&&&&&&&&&.................&&...........................
 * @...&&&&&&&&&&&&&&&.....................&&&&......................
 * @...&&&&&&&&&&.&&&........................&&&&&...................
 * @..&&&&&&&&&&&..&&..........................&&&&&&&...............
 * @..&&&&&&&&&&&&...&............&&&.....&&&&...&&&&&&&.............
 * @..&&&&&&&&&&&&&.................&&&.....&&&&&&&&&&&&&&...........
 * @..&&&&&&&&&&&&&&&&..............&&&&&&&&&&&&&&&&&&&&&&&&.........
 * @..&&.&&&&&&&&&&&&&&&&&.........&&&&&&&&&&&&&&&&&&&&&&&&&&&.......
 * @...&&..&&&&&&&&&&&&.........&&&&&&&&&&&&&&&&...&&&&&&&&&&&&......
 * @....&..&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&...........&&&&&&&&.....
 * @.......&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&..............&&&&&&&....
 * @.......&&&&&.&&&&&&&&&&&&&&&&&&..&&&&&&&&...&..........&&&&&&....
 * @........&&&.....&&&&&&&&&&&&&.....&&&&&&&&&&...........&..&&&&...
 * @.......&&&........&&&.&&&&&&&&&.....&&&&&.................&&&&...
 * @.......&&&...............&&&&&&&.......&&&&&&&&............&&&...
 * @........&&...................&&&&&&.........................&&&..
 * @.........&.....................&&&&........................&&....
 * @...............................&&&.......................&&......
 * @................................&&......................&&.......
 * @.................................&&..............................
 * @..................................&..............................
 * @
 * @Author: Wang Haitao
 * @Date: 2021-12-24 10:10:27
 * @LastEditTime: 2021-12-24 11:35:18
 * @LastEditors: Wang Haitao
 * @FilePath: /titan/src/delta_compression.h
 * @Description: https://github.com/apple-ouyang
 */

#pragma once

#include "blob_format.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch_base.h"
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
struct SuperFeaturesSet {
  super_feature_t super_features[NUM_SUPER_FEATURE];
};

using std::forward_list;
using std::map;
using std::string;
using std::vector;

class FeatureIndexTable {
public:
  void Put(const Slice &key, const Slice &value);

  // 删除 key 对应的三个特征，以及三个特征对应的 key
  void Delete(const Slice &key);

  void RangeDelete(const Slice &start, const Slice &end);

  Status Write(WriteBatch *updates);

  bool inline IsKeyExist(const Slice &key) { IsKeyExist(key.ToString()); }

  //查找 key 对应的记录相似记录的 similar_keys
  bool FindKeysOfSimilarRecords(const Slice &key, vector<string> &similar_keys);

private:
  // TODO:除了链表还有什么高效的结构吗？
  map<super_feature_t, forward_list<string>> feature_key_tbl;
  map<string, SuperFeaturesSet> key_feature_tbl;

  // 删除key对应的特征（默认3个）
  void DeleteFeaturesOfAKey(const string &key, const SuperFeaturesSet &sfs);

  void Delete(const string &key);

  bool inline IsKeyExist(const string &key) {
    return key_feature_tbl.find(key) != key_feature_tbl.end();
  }
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
  // just 256 random numbers to map from 256 ASCILL characters
  static uint64_t rand_nums[256];

  /**
   * @description: Detect records similarity. Then we can use delta compression
   * to compress the similar values.
   * @param feature_num number of features to generate. should be multiply of
   * NUM_SUPER_FEATURE.
   */
  FeatureSample(uint8_t features_num = 12);
  ~FeatureSample();

  SuperFeaturesSet GenerateFeatures(const Slice &value);

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
  void IndexFeatures(SuperFeaturesSet *sfs);

  uint8_t features_num_;
  uint64_t *features_;
  uint64_t *transform_args_a_; // random numbers
  uint64_t *transform_args_b_;
};

enum DeltaCompressMethod { xdelta, edelta, gdelta };

bool DeltaCompress(const Slice *base, size_t num, const PinnableSlice *values,
                   vector<Slice> delta, DeltaCompressMethod method = xdelta);

// 全局变量： TODO：暂时不知道放哪里
extern FeatureIndexTable feature_idx_tbl;

} // namespace titandb
} // namespace rocksdb
