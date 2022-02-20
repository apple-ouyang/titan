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
 * @Date: 2021-12-24 11:04:22
 * @LastEditTime: 2021-12-24 11:05:41
 * @LastEditors: Wang Haitao
 * @FilePath: /titan/src/delta_compression.cpp
 * @Description: https://github.com/apple-ouyang
 */

#include "delta_compression.h"
#include "gdelta.h"
#include "gear-matrix.h"
#include "util/xxhash.h"
#include "xdelta3.h"

#include <cstdint>
#include <random>

namespace rocksdb {
namespace titandb {

FeatureIndexTable feature_idx_tbl;

void FeatureIndexTable::DeleteFeaturesOfAKey(const string &key,
                                             const SuperFeaturesSet &sfs) {
  for (const auto &sf : sfs.super_features) {
    assert(feature_key_tbl.find(sf) != feature_key_tbl.end());

    // 遍历单链表，找到对应的索引删除，然后立马退出
    // 这里没有使用 forward_list.remove 是因为只可能有一个需要删除
    // 删除后直接退出即可，不用遍历全部链表
    for (auto it = feature_key_tbl[sf].before_begin();
         it != feature_key_tbl[sf].end();) {
      auto nex = next(it);
      if (*nex == key) {
        feature_key_tbl[sf].erase_after(it);
        if (feature_key_tbl[sf].empty())
          feature_key_tbl.erase(sf);
        break;
      }
      it = nex;
    }
  }
}

void FeatureIndexTable::Delete(const string &key) {
  SuperFeaturesSet sfs = key_feature_tbl.at(key);
  DeleteFeaturesOfAKey(key, sfs);
  key_feature_tbl.erase(key);
}

void FeatureIndexTable::Delete(const Slice &key) { Delete(key.ToString()); }

void FeatureIndexTable::RangeDelete(const Slice &start, const Slice &end) {
  auto it_start = key_feature_tbl.find(start.ToString());
  auto it_end = key_feature_tbl.find(end.ToString());
  for (auto it = it_start; it != it_end; ++it) {
    DeleteFeaturesOfAKey(it->first, it->second);
  }
  key_feature_tbl.erase(it_start, it_end);
}

void FeatureIndexTable::Put(const Slice &key, const Slice &value) {
  const string k = key.ToString();
  if (IsKeyExist(k)) {
    Delete(k);
  }

  FeatureSample fs;
  auto sfs = fs.GenerateFeatures(value);
  key_feature_tbl[k] = sfs;
  for (const auto &sf : sfs.super_features) {
    feature_key_tbl[sf].push_front(k);
  }
}

Status FeatureIndexTable::Write(WriteBatch *updates) {
  FeatureHandle hd;
  return updates->Iterate(&hd);
}

bool FeatureIndexTable::FindKeysOfSimilarRecords(const Slice &key,
                                                 vector<string> &similar_keys) {
  const string k = key.ToString();
  SuperFeaturesSet sfs = key_feature_tbl.at(k);
  bool find_similar_keys = false;

  for (const auto &sf : sfs.super_features) {
    for (const string &similar_key : feature_key_tbl[sf]) {
      if (similar_key != k) {
        similar_keys.emplace_back(similar_key);
        find_similar_keys = true;
      }
    }
  }

  //如果找到相似记录，则将所有相似的记录从相似索引表中删除
  if (find_similar_keys) {
    for (const string &similar_key : similar_keys) {
      Delete(similar_key);
    }
  }
  return find_similar_keys;
}

FeatureSample::FeatureSample(uint8_t features_num)
    : features_num_(features_num) {
  std::random_device rd;
  std::default_random_engine e(rd());
  std::uniform_int_distribution<uint64_t> dis(0, UINT64_MAX);

  features_ = (uint64_t *)malloc(sizeof(uint64_t) * features_num_);
  transform_args_a_ = (uint64_t *)malloc(sizeof(uint64_t) * features_num_);
  transform_args_b_ = (uint64_t *)malloc(sizeof(uint64_t) * features_num_);

  for (size_t i = 0; i < features_num_; ++i) {
    transform_args_a_[i] = dis(e);
    transform_args_b_[i] = dis(e);
    features_[i] = 0;
  }
}

FeatureSample::~FeatureSample() {
  free(features_);
  free(transform_args_a_);
  free(transform_args_b_);
}

void FeatureSample::OdessResemblanceDetect(const Slice &value) {
  uint64_t hash = 0;
  for (size_t i = 0; i < value.size(); ++i) {
    hash = (hash << 1) + gear_matrix[static_cast<uint8_t>(value[i])];
    if (!(hash & SAMPLE_MASK)) {
      for (size_t j = 0; j < features_num_; ++j) {
        uint64_t transform_res =
            hash * transform_args_a_[j] + transform_args_b_[j];
        if (transform_res > features_[j])
          features_[j] = transform_res;
      }
    }
  }
}

void FeatureSample::IndexFeatures(SuperFeaturesSet *sfs) {
  for (int i = 0; i < NUM_SUPER_FEATURE; ++i) {
    size_t group_len = features_num_ / NUM_SUPER_FEATURE;
    sfs->super_features[i] = XXH64(&this->features_[i * group_len],
                                   sizeof(*features_) * group_len, 0x7fcaf1);
  }
}

SuperFeaturesSet FeatureSample::GenerateFeatures(const Slice &value) {
  this->OdessResemblanceDetect(value);
  SuperFeaturesSet sfs;
  IndexFeatures(&sfs);
  return sfs;
}

int XdeltaCompress(const Slice &src, const Slice &in, string &delta) {
  const size_t MAX_OUTPUT = 2 * in.size();
  uint8_t *out = new uint8_t[MAX_OUTPUT];
  size_t out_size;
  int status =
      xd3_encode_memory((uint8_t *)in.data(), in.size(), (uint8_t *)src.data(),
                        src.size(), out, &out_size, MAX_OUTPUT, 0);
  delta.assign((char *)out, out_size);
  return status;
}

bool DeltaCompress(const Slice &base, size_t num, const PinnableSlice *inputs,
                   std::vector<string> &deltas, DeltaCompressMethod method) {
  size_t max_input_len = 0;
  for (size_t i = 0; i < num; ++i) {
    max_input_len = std::max(max_input_len, inputs[i].size());
  }
  const size_t MAX_BUF_LEN = max_input_len * 2;
  uint8_t *buf = new uint8_t[MAX_BUF_LEN];
  size_t buf_len;

  for (size_t i = 0; i < num; ++i) {
    switch (method) {
    case xdelta: {
      xd3_encode_memory((uint8_t *)inputs[i].data(), inputs[i].size(),
                        (uint8_t *)base.data(), base.size(), buf, &buf_len,
                        MAX_BUF_LEN, 0);
      break;
    }
    case edelta: {
      break;
    }
    case gdelta: {
      gencode((uint8_t *)inputs[i].data(), inputs[i].size(),
              (uint8_t *)base.data(), base.size(), buf, (uint32_t *)&buf_len);
      break;
    }
    default:
      break;
    }
    deltas[i].assign((char *)buf, buf_len);
  }
  delete[] buf;
  return true;
}

} // namespace titandb
} // namespace rocksdb