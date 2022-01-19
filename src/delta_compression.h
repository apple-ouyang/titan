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
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/xxhash.h"
#include <cstdint>
#include <forward_list>
#include <map>

#define NUM_SUPER_FEATURE                                                      \
  3 // Number of super features that each record will have.

#define SAMPLE_MASK                                                            \
  0x0000400303410000 // It has 7 bits of 1's, so the sample rate is
                     // 1/(2^7)=1/128. It means the number of sampled chunks to
                     // generate feature will be 1/128 of the all sliding window
                     // chunks.

typedef rocksdb::XXH64_hash_t super_feature_t;

class FeatureIndexTable{
  public:
  bool IsHaveSimilar(super_feature_t sf){
    return !this->feature_key_tbl[sf].empty();
  }

  void Insert(super_feature_t sf, const rocksdb::Slice & key){
    feature_key_tbl[sf].push_front(key);
  }

  // 遍历单链表，找到对应的索引删除
  void Delete(const rocksdb::Slice & key){
    super_feature_t sf = key_feature_tbl[key];
    for(auto it = feature_key_tbl[sf].before_begin(); it != feature_key_tbl[sf].end(); ){
      auto nex = next(it);
      if(*nex == key){
        feature_key_tbl[sf].erase_after(it);
        break;
      }
      it = nex;
    }
    key_feature_tbl.erase(key_feature_tbl.find(key));
  }

  void DeleteRange(const rocksdb::Slice & start, const rocksdb::Slice & end){
    
  }

  private:
  // TODO:除了链表还有什么高效的结构吗？
  std::map<super_feature_t, std::forward_list<rocksdb::Slice> > feature_key_tbl;
  std::map<rocksdb::Slice, super_feature_t> key_feature_tbl;
};



// The super feature are used for similarity detection. The more of super
// features a record have, the bigger feature index table will be.
struct SuperFeaturesSet {
  super_feature_t super_features[NUM_SUPER_FEATURE];
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

  SuperFeaturesSet GenerateFeatures(const rocksdb::Slice &value);

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
  void OdessResemblanceDetect(const rocksdb::Slice &value);

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
