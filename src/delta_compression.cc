#include "delta_compression.h"
#include "edelta.h"
#include "gdelta.h"
#include "gear-matrix.h"
#include "util/coding.h"
#include "util/mutexlock.h"
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
void FeatureIndexTable::Put(const Slice &key, const Slice &value) {
  if (value.size() < min_blob_size_) {
    // Delta compression is happend in GC. GC only process records in blob file.
    // Only records whose value size >= min_blob_size_ will store in blob file.
    // So we just need to store those big value records' feature
    return;
  }

  const string k = key.ToString();
  SuperFeatures super_features;
  MutexLock l(&mutex_);

  // replace old feature with a new feature
  DeleteIfExist(k);

  super_features = feature_generator_.GenerateSuperFeatures(value);

  key_feature_table_[k] = super_features;
  for (const auto &sf : super_features) {
    feature_key_table_[sf].insert(k);
  }
}

void FeatureIndexTable::Delete(const Slice &key) {
  MutexLock l(&mutex_);
  DeleteIfExist(key.ToString());
}

void FeatureIndexTable::DeleteIfExist(const string &key) {
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
  MutexLock l(&mutex_);
  auto it_start = key_feature_table_.find(start.ToString());
  auto it_end = key_feature_table_.find(end.ToString());
  for (auto key_feature = it_start; key_feature != it_end; ++key_feature) {
    auto key = key_feature->first;
    auto super_features = key_feature->second;
    for (const auto &sf : super_features) {
      feature_key_table_[sf].erase(key);
    }
  }
  key_feature_table_.erase(it_start, it_end);
}

// Status FeatureIndexTable::Write(WriteBatch *updates) {
//   // TODO(haitao) 确定可以吗？可以写batch？
//   // TODO(haitao)
//   // 会不会被其他函数调用过导致重复写index？比如Put、Delte调用write？
//   FeatureHandle hd;
//   return updates->Iterate(&hd);
// }

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

size_t FeatureIndexTable::CountAllSimilarRecords() const {
  MutexLock l(&mutex_);
  size_t num = 0;
  unordered_set<string> similar_keys;
  for (auto it : feature_key_table_) {
    auto keys = it.second;

    // If there are more than one records have the same feature,
    // thoese Values corresponding to the keys are considered similar
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

  MutexLock l(&mutex_);
  if (!GetSuperFeatures(k, &super_features)) {
    return;
  }

  for (const auto &sf : super_features) {
    for (const string &similar_key : feature_key_table_[sf]) {
      if (similar_key != k) {
        similar_keys.push_back(similar_key);
      }
    }
  }

  for (const string &similar_key : similar_keys) {
    DeleteIfExist(similar_key);
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
#ifdef FIX_TRANSFORM_ARGUMENT_TO_KEEP_SAME_SIMILARITY_DETECTION_BETWEEN_TESTS
    random_transform_args_a_[i] = gear_matrix[i];
    random_transform_args_b_[i] = gear_matrix[kFeatureNumber + i];
#else
    random_transform_args_a_[i] = dis(e);
    random_transform_args_b_[i] = dis(e);
#endif
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

void FeatureGenerator::MakeSuperFeatures() {
  if (kSuperFeatureNumber == kFeatureNumber)
    CopyFeaturesAsSuperFeatures();
  else
    GroupFeaturesAsSuperFeatures();
}

void FeatureGenerator::CopyFeaturesAsSuperFeatures() {
  super_features_ = features_;
}

// Divede features into groups, then use the group hash as the super feature
void FeatureGenerator::GroupFeaturesAsSuperFeatures() {
  super_features_.resize(kSuperFeatureNumber);
  SuperFeatures super_features(kSuperFeatureNumber);
  for (size_t i = 0; i < kSuperFeatureNumber; ++i) {
    size_t group_len = kFeatureNumber / kSuperFeatureNumber;
    super_features_[i] = XXH64(&features_[i * group_len],
                               sizeof(feature_t) * group_len, 0x7fcaf1);
  }
}

void FeatureGenerator::CleanFeatures() {
  std::fill(features_.begin(), features_.end(), 0);
}

SuperFeatures FeatureGenerator::GenerateSuperFeatures(const Slice &value) {
  CleanFeatures();
  OdessResemblanceDetect(value);
  MakeSuperFeatures();
  return super_features_;
}

bool XDelta_Compress(const char *input, size_t input_len, const char *base,
                     size_t base_len, ::std::string *output) {
#ifdef _XDELTA3_H_ // TODO(haitao)
                   // 可能是XDELTA，需要看一下这里的逻辑，比如cmake文件，先这样写把
  if (input_len == 0 || base_len == 0)
    return false;
  const size_t kMaxOutLen = input_len * 2;
  unsigned char *buff = new unsigned char[kMaxOutLen];
  size_t outlen = 0;
  int s = xd3_encode_memory((uint8_t *)input, input_len, (uint8_t *)base,
                            base_len, (uint8_t *)buff, &outlen, kMaxOutLen, 0);
  output->assign(buff, buff + outlen);
  delete[] buff;
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
  if (input_len > std::numeric_limits<uint32_t>::max() ||
      base_len > std::numeric_limits<uint32_t>::max()) {
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
  output->assign(buff, buff + outlen);
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
  EDeltaDecode((uint8_t *)delta, delta_len, (uint8_t *)base, (uint32_t)base_len,
               (uint8_t *)output, outlen);
  return true;
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
  if (input_len > std::numeric_limits<uint32_t>::max() ||
      base_len > std::numeric_limits<uint32_t>::max()) {
    // Can't compress more than 4GB
    return false;
  }
  if (input_len == 0 || base_len == 0)
    return false;
  const size_t kMaxOutLen = input_len * 2;
  unsigned char *buff = new unsigned char[kMaxOutLen];
  uint32_t outlen = 0;
  gencode((uint8_t *)input, input_len, (uint8_t *)base, (uint32_t)base_len,
          (uint8_t **)&buff, &outlen);
  output->assign(buff, buff + outlen);
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
  gdecode((uint8_t *)delta, delta_len, (uint8_t *)base, (uint32_t)base_len,
          (uint8_t **)&output, outlen);
  return true;
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
  bool ok = false;

  string compressed;
  switch (type) {
  case kXDelta: {
    if (XDelta_Compress(input.data(), input.size(), base.data(), base.size(),
                        &compressed) &&
        GoodCompressionRatio(compressed.size(), input.size())) {
      ok = true;
    }
    break;
  }
  case kEDelta: {
    if (EDelta_Compress(input.data(), input.size(), base.data(), base.size(),
                        &compressed) &&
        GoodCompressionRatio(compressed.size(), input.size())) {
      ok = true;
    }
    break;
  }
  case kGDelta: {
    if (GDelta_Compress(input.data(), input.size(), base.data(), base.size(),
                        &compressed) &&
        GoodCompressionRatio(compressed.size(), input.size())) {
      ok = true;
    }
    break;
  }
  default: {
  } // Do not recognize this compression type
  }

  if (ok)
    output->append(compressed);

  return ok;
}

Status DeltaUncompress(DeltaCompressType type, const Slice &delta,
                       const Slice &base, OwnedSlice *output) {
  if (delta.empty() || base.empty())
    return Status::Corruption("Delta Uncompress Failed", "Delta or base empty");
  CacheAllocationPtr ubuf;
  uint32_t original_length;
  Slice delta_copy(delta);

  if (!GetVarint32(&delta_copy, &original_length)) {
    return Status::Corruption("Currupted delta compression", "original_length");
  }

  ubuf.reset(new char[original_length]);
  size_t output_length;
  assert(type != kNoDeltaCompression);

  switch (type) {
  case kXDelta: {
    if (!XDelta_Uncompress(delta_copy.data(), delta_copy.size(), base.data(),
                           base.size(), ubuf.get(), &output_length,
                           original_length)) {
      return Status::Corruption("Corrupted compressed blob", "XDelta");
    }
    output->reset(std::move(ubuf), output_length);
    break;
  }
  case kEDelta:
    if (!EDelta_Uncompress(delta.data(), delta.size(), base.data(), base.size(),
                           ubuf.get(), (uint32_t *)&output_length)) {
      return Status::Corruption("Corrupted compressed blob", "EDelta");
    }
    output->reset(std::move(ubuf), output_length);
    break;
  case kGDelta:
    if (!GDelta_Uncompress(delta.data(), delta.size(), base.data(), base.size(),
                           ubuf.get(), (uint32_t *)&output_length)) {
      return Status::Corruption("Corrupted compressed blob", "GDelta");
    }
    output->reset(std::move(ubuf), output_length);
    break;
  default:
    return Status::Corruption("bad delta compression type");
  }

  if (output_length != original_length)
    return Status::Corruption("Delta Compression corrupted",
                              "output_length != original_length");

  return Status::OK();
}

} // namespace titandb
} // namespace rocksdb