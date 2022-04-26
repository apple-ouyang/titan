#include "delta_compression.h"
#include "edelta.h"
#include "gdelta.h"
#include "gear-matrix.h"
#include "util/coding.h"
#include "util/xxhash.h"
#include "xdelta3.h"
#include <bits/types/struct_timespec.h>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <random>
#include <sys/select.h>
#include <unordered_set>

namespace rocksdb {
namespace titandb {

FeatureIndexTable feature_index_table;

void FeatureIndexTable::Delete(const string &key) {
  SuperFeatures super_features;
  if (GetSuperFeatures(key, &super_features)) {
    ExecuteDelete(key, super_features);
  }
}

void FeatureIndexTable::ExecuteDelete(const string &key,
                                      const SuperFeatures &super_features) {
  for (const auto &sf : super_features) {
    feature_key_table_[sf].erase(key);
  }
  key_feature_table_.erase(key);
}

void FeatureIndexTable::RangeDelete(const Slice &start, const Slice &end) {
  auto it_start = key_feature_table_.find(start.ToString());
  auto it_end = key_feature_table_.find(end.ToString());
  for (auto key_feature = it_start; key_feature != it_end; ++key_feature) {
    auto key = key_feature->first;
    auto super_features = key_feature->second;
    for (auto f : super_features) {
      feature_key_table_[f].erase(key);
    }
  }
  key_feature_table_.erase(it_start, it_end);
}

void FeatureIndexTable::Put(const Slice &key, const Slice &value) {
  const string k = key.ToString();
  SuperFeatures super_features;
  // delete old feature if it exits so we can insert a new one
  Delete(k);

  super_features = feature_generator_.GenerateSuperFeatures(value);
  key_feature_table_[k] = super_features;
  for (const auto &sf : super_features) {
    feature_key_table_[sf].insert(k);
  }
}

Status FeatureIndexTable::Write(WriteBatch *updates) {
  // TODO(haitao) 确定可以吗？可以写batch？
  // TODO(haitao)
  // 会不会被其他函数调用过导致重复写index？比如Put、Delte调用write？
  FeatureHandle hd;
  return updates->Iterate(&hd);
}

bool FeatureIndexTable::GetSuperFeatures(const string &key,
                                         SuperFeatures *super_features) {
  auto it = key_feature_table_.find(key);
  if (it == key_feature_table_.end()) {
    return false;
  } else {
    *super_features = it->second;
    return true;
  }
}

size_t FeatureIndexTable::CountAllSimilarRecords() {
  size_t num = 0;
  unordered_set<string> similar_keys;
  for (auto it : feature_key_table_) {
    auto keys = it.second;

    // If there are more than one records have the same feature,
    // thoese keys are considered similar
    if (keys.size() > 1) {
      for (const string &key : keys)
        similar_keys.emplace(move(key));
    }
  }
  return similar_keys.size();
}

void FeatureIndexTable::GetSimilarRecordsKeys(const Slice &key,
                                              vector<string> &similar_keys) {
  SuperFeatures super_features;
  const string k = key.ToString();
  if (!GetSuperFeatures(k, &super_features)) {
    return;
  }

  for (const auto &sf : super_features) {
    for (string similar_key : feature_key_table_[sf]) {
      if (similar_key != k) {
        similar_keys.emplace_back(move(similar_key));
      }
    }
  }

  for (const string &similar_key : similar_keys) {
    Delete(similar_key);
  }
  ExecuteDelete(k, super_features);
}

FeatureGenerator::FeatureGenerator(feature_t sample_mask, size_t feature_number,
                                   size_t super_feature_number)
    : kSampleRatioMask(sample_mask), kFeatureNumber(feature_number),
      kSuperFeatureNumber(super_feature_number) {
  assert(kFeatureNumber % kSuperFeatureNumber == 0);

  std::random_device rd;
  std::default_random_engine e(rd());
  std::uniform_int_distribution<feature_t> dis(0, UINT64_MAX);

  features_.resize(kFeatureNumber);
  random_transform_args_a_.resize(kFeatureNumber);
  random_transform_args_b_.resize(kFeatureNumber);

  for (size_t i = 0; i < kFeatureNumber; ++i) {
    random_transform_args_a_[i] = dis(e);
    random_transform_args_b_[i] = dis(e);
    features_[i] = 0;
  }
}

void FeatureGenerator::OdessResemblanceDetect(const Slice &value) {
  feature_t hash = 0;
  for (size_t i = 0; i < value.size(); ++i) {
    hash = (hash << 1) + gear_matrix[static_cast<uint8_t>(value[i])];
    if (!(hash & kSampleRatioMask)) {
      for (size_t j = 0; j < kFeatureNumber; ++j) {
        feature_t transform_res =
            hash * random_transform_args_a_[j] + random_transform_args_b_[j];
        if (transform_res > features_[j])
          features_[j] = transform_res;
      }
    }
  }
}

SuperFeatures FeatureGenerator::MakeSuperFeatures() {
  if (kSuperFeatureNumber == kFeatureNumber)
    return CopyFeaturesAsSuperFeatures();
  else
    return GroupFeaturesAsSuperFeatures();
}

SuperFeatures FeatureGenerator::CopyFeaturesAsSuperFeatures() {
  SuperFeatures super_features(features_.begin(), features_.end());
  return super_features;
}

// Divede features into groups, then use the group hash as the super feature
SuperFeatures FeatureGenerator::GroupFeaturesAsSuperFeatures() {
  SuperFeatures super_features(kSuperFeatureNumber);
  for (size_t i = 0; i < kSuperFeatureNumber; ++i) {
    size_t group_len = kFeatureNumber / kSuperFeatureNumber;
    super_features[i] = XXH64(&features_[i * group_len],
                              sizeof(feature_t) * group_len, 0x7fcaf1);
  }
  return super_features;
}

void FeatureGenerator::CleanFeatures() {
  for (size_t i = 0; i < kFeatureNumber; ++i) {
    features_[i] = 0;
  }
}

SuperFeatures FeatureGenerator::GenerateSuperFeatures(const Slice &value) {
  CleanFeatures();
  OdessResemblanceDetect(value);
  return MakeSuperFeatures();
}

bool XDelta_Compress(const char *input, size_t input_len, const char *base,
                     size_t base_len, ::std::string *output) {
#ifdef _XDELTA3_H_ // TODO(haitao)
                   // 可能是XDELTA，需要看一下这里的逻辑，比如cmake文件，先这样写把
  if (input_len == 0 || base_len == 0)
    return false;
  const size_t kMaxOutLen = input_len * 2;
  unsigned char *buff = new unsigned char[kMaxOutLen];
  // output->resize(kMaxOutLen);
  size_t outlen;
  int s = xd3_encode_memory((uint8_t *)input, input_len, (uint8_t *)base,
                            base_len, (uint8_t *)buff, &outlen, kMaxOutLen, 0);
  output->clear();
  *output = string(buff, buff + outlen);
  delete[] buff;
  // output->assign(buff, outlen);
  // output->resize(outlen);
  return s == 0;
#else
  (void)input;
  (void)length;
  (void)output;
  return false;
#endif
}

bool XDelta_Uncompress(const char *delta, size_t delta_len, const char *base,
                       size_t base_len, char *output, size_t *outlen,
                       uint32_t original_length) {
#ifdef _XDELTA3_H_ // TODO(haitao)
  return xd3_decode_memory((uint8_t *)delta, delta_len, (uint8_t *)base,
                           base_len, (uint8_t *)output, outlen, original_length,
                           0) == 0;
#else
  (void)input;
  (void)length;
  (void)output;
  return false;
#endif
}

bool EDelta_Compress(const char *input, size_t input_len, const char *base,
                     size_t base_len, ::std::string *output) {
#ifdef _EDELTA_H // TODO(haitao) 可能是 EDELTA
                 // ，需要看一下这里的逻辑，比如cmake文件，先这样写把
  if (input_len >
      std::numeric_limits<uint32_t>::max()) { // TODO(haitao) 确认一下
    // Can't compress more than 4GB
    return false;
  }
  if (input_len == 0 || base_len == 0)
    return false;
  const size_t kMaxOutLen = input_len * 2;
  unsigned char *buff = new unsigned char[kMaxOutLen];
  uint32_t outlen = 0;
  EDeltaEncode((uint8_t *)input, input_len, (uint8_t *)base, (uint32_t)base_len,
               (uint8_t *)buff, &outlen);
  *output = string(buff, buff + outlen);
  delete[] buff;
  return true; // TODO(haitao) 需要看一下到底是不是 == 0，大概率是的
#else
  (void)input;
  (void)length;
  (void)output;
  return false;
#endif
}

bool EDelta_Uncompress(const char *delta, size_t delta_len, const char *base,
                       size_t base_len, char *output, uint32_t *outlen) {
#ifdef _EDELTA_H // TODO(haitao) 可能是 EDELTA
                 // ，需要看一下这里的逻辑，比如cmake文件，先这样写把
  return EDeltaDecode((uint8_t *)delta, delta_len, (uint8_t *)base,
                      (uint32_t)base_len, (uint8_t *)output, outlen) == 0;
#else
  (void)input;
  (void)length;
  (void)output;
  return false;
#endif
}

bool GDelta_Compress(const char *input, size_t input_len, const char *base,
                     size_t base_len, ::std::string *output) {
#ifdef GDELTA_GDELTA_H // TODO(haitao)
  // 可能是XDELTA，需要看一下这里的逻辑，比如cmake文件，先这样写把
  if (input_len >
      std::numeric_limits<uint32_t>::max()) { // TODO(haitao) 确认一下
    // Can't compress more than 4GB
    return false;
  }
  if (input_len == 0 || base_len == 0)
    return false;
  const size_t kMaxOutLen = input_len * 2;
  unsigned char *buff = new unsigned char[kMaxOutLen];
  size_t outlen = 0;
  uint32_t delta_size;
  outlen = gencode((uint8_t *)input, input_len, (uint8_t *)base,
                   (uint32_t)base_len, (uint8_t *)buff, &delta_size);
  *output = string(buff, buff + outlen);
  delete[] buff;
  return true; // TODO(haitao) 需要看一下到底是不是 == 0，大概率是的
#else
  (void)input;
  (void)length;
  (void)output;
  return false;
#endif
}

bool GDelta_Uncompress(const char *delta, size_t delta_len, const char *base,
                       size_t base_len, char *output, uint32_t *outlen) {
#ifdef GDELTA_GDELTA_H // TODO(haitao)
  return gdecode((uint8_t *)delta, delta_len, (uint8_t *)base,
                 (uint32_t)base_len, (uint8_t *)output, outlen) == 0;
#else
  (void)input;
  (void)length;
  (void)output;
  return false;
#endif
}

// Delta compressed delta format:
//
//    +---------------------+------------------+
//    |   original_length   | compressed value |
//    +---------------------+------------------+
//    |       Varint32      |                  |
//    +---------------------+------------------+

bool DeltaCompress(DeltaCompressType type, const Slice &input,
                   const Slice &base, std::string *output) {
  if (type == kNoDeltaCompression) {
    return false;
  }

  uint32_t original_length = input.size();
  PutVarint32(output, original_length);

  string compressed;
  switch (type) {
  case kXDelta:
    if (XDelta_Compress(input.data(), input.size(), base.data(), base.size(),
                        &compressed) &&
        GoodCompressionRatio(compressed.size(), input.size())) {
      output->append(compressed);
      return true;
    }
    break;
  case kEDelta:
    if (EDelta_Compress(input.data(), input.size(), base.data(), base.size(),
                        &compressed) &&
        GoodCompressionRatio(compressed.size(), input.size())) {
      output->append(compressed);
      return true;
    }
    break;
  case kGDelta:
    if (GDelta_Compress(input.data(), input.size(), base.data(), base.size(),
                        &compressed) &&
        GoodCompressionRatio(compressed.size(), input.size())) {
      output->append(compressed);
      return true;
    }
    break;
  default: {
  } // Do not recognize this compression type
  }

  // TODO(haitao) 写个log
  return false;
}

Status DeltaUncompress(DeltaCompressType type, Slice delta, Slice base,
                       OwnedSlice *output) {
  int size = 0;
  CacheAllocationPtr ubuf;
  uint32_t original_length;

  if (!GetVarint32(&delta, &original_length)) {
    return Status::Corruption("Currupted delta compression", "original_length");
  }

  ubuf.reset(new char[original_length]);
  size_t decompressed_length;
  assert(type != kNoDeltaCompression);

  switch (type) {
  case kXDelta: {
    if (!XDelta_Uncompress(delta.data(), delta.size(), base.data(),
                           base.size(), ubuf.get(), &decompressed_length,
                           original_length)) {
      return Status::Corruption("Corrupted compressed blob", "XDelta");
    }
    output->reset(std::move(ubuf), size);
    break;
  }
  case kEDelta:
    if (!EDelta_Uncompress(delta.data(), delta.size(), base.data(),
                           base.size(), ubuf.get(),
                           (uint32_t *)&decompressed_length)) {
      return Status::Corruption("Corrupted compressed blob", "EDelta");
    }
    output->reset(std::move(ubuf), size);
    break;
  case kGDelta:
    if (!GDelta_Uncompress(delta.data(), delta.size(), base.data(),
                           base.size(), ubuf.get(),
                           (uint32_t *)&decompressed_length)) {
      return Status::Corruption("Corrupted compressed blob", "GDelta");
    }
    output->reset(std::move(ubuf), size);
    break;
  default:
    return Status::Corruption("bad delta compression type");
  }

  if (decompressed_length != original_length)
    return Status::Corruption("Delta Compression corrupted",
                              "length"); // TODO 换个好名字

  return Status::OK();
}

} // namespace titandb
} // namespace rocksdb