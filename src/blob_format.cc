#include "blob_format.h"

#include "delta_compression.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "titan/options.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace rocksdb {
namespace titandb {

namespace {

bool GetChar(Slice* src, unsigned char* value) {
  if (src->size() < 1) return false;
  *value = *src->data();
  src->remove_prefix(1);
  return true;
}

}  // namespace

void BlobRecord::EncodeTo(std::string* dst) const {
  PutLengthPrefixedSlice(dst, key);
  PutLengthPrefixedSlice(dst, value);
}

Status BlobRecord::DecodeFrom(Slice* src) {
  if (!GetLengthPrefixedSlice(src, &key) ||
      !GetLengthPrefixedSlice(src, &value)) {
    return Status::Corruption("BlobRecord");
  }
  return Status::OK();
}

bool operator==(const BlobRecord& lhs, const BlobRecord& rhs) {
  return lhs.key == rhs.key && lhs.value == rhs.value;
}

void DeltaRecords::EncodeTo(std::string *dst) const {
  BlobRecord::EncodeTo(dst);
  for (size_t i = 0; i < deltas_keys.size(); ++i) {
    PutLengthPrefixedSlice(dst, deltas_keys[i]);
    PutLengthPrefixedSlice(dst, deltas_values[i]);
  }
}

Status DeltaRecords::DecodeFrom(Slice *src) {
  Status s = BlobRecord::DecodeFrom(src);
  if(!s.ok())
    return s;
  while (src->size() > 0) {
    Slice delta_key, delta_value;
    if (!GetLengthPrefixedSlice(src, &delta_key) ||
        !GetLengthPrefixedSlice(src, &delta_value)) {
      return Status::Corruption("DeltaRecords");
    }
    deltas_keys.emplace_back(std::move(delta_key));
    deltas_values.emplace_back(std::move(delta_value));
  }
  return Status::OK();
}

size_t DeltaRecords::size() const {
  size_t size = BlobRecord::size();
  for (size_t i = 0; i < deltas_keys.size(); ++i) {
    size += deltas_keys[i].size() + deltas_values[i].size();
  }
  return size;
}

template <typename BlobType>
void BlobEncoder::EncodeRecordTemplate(const BlobType &record){
  record_buffer_.clear();
  record.EncodeTo(&record_buffer_);
  CompressAndEncodeHeader(record_buffer_);
}

void BlobEncoder::EncodeRecord(const BlobRecord &record){
  SetIsDeltaCompressed(false);
  EncodeRecordTemplate<BlobRecord>(record);
}

void BlobEncoder::EncodeDeltaRecords(const DeltaRecords &records){
  SetIsDeltaCompressed(true);
  EncodeRecordTemplate<DeltaRecords>(records);
}

void BlobEncoder::CompressAndEncodeHeader(const Slice& record) {
  compressed_buffer_.clear();
  CompressionType compression;
  record_ =
      Compress(*compression_info_, record, &compressed_buffer_, &compression);

  EncodeHeader(compression);
}

void BlobEncoder::EncodeHeader(CompressionType compression){
  assert(record_.size() < std::numeric_limits<uint32_t>::max());
  EncodeFixed32(header_ + 4, static_cast<uint32_t>(record_.size()));
  header_[8] = compression;
  header_[9] = is_delta_compressed_ ? delta_compression_ : kNoDeltaCompression;

  uint32_t crc = crc32c::Value(header_ + 4, sizeof(header_) - 4);
  crc = crc32c::Extend(crc, record_.data(), record_.size());
  EncodeFixed32(header_, crc);
}

Status BlobDecoder::DecodeHeader(Slice* src) {
  if (!GetFixed32(src, &crc_)) {
    return Status::Corruption("BlobHeader");
  }
  header_crc_ = crc32c::Value(src->data(), kRecordHeaderSize - 4);

  unsigned char compression, delta_compression;
  if (!GetFixed32(src, &record_size_) || !GetChar(src, &compression) ||
      !GetChar(src, &delta_compression)) {
    return Status::Corruption("BlobHeader");
  }

  compression_ = static_cast<CompressionType>(compression);
  delta_compression_ = static_cast<DeltaCompressType>(delta_compression);
  is_delta_records_ = delta_compression != kNoDeltaCompression;
  return Status::OK();
}

Status BlobDecoder::CheckRecordCrc(const Slice &src){
  uint32_t crc = crc32c::Extend(header_crc_, src.data(), src.size());
  if (crc != crc_) {
    return Status::Corruption("BlobDecoder", "checksum mismatch");
  }
  return Status::OK();
}

Status BlobDecoder::UncompressRecordIntoBuffer(const Slice &src, OwnedSlice* buffer){
  UncompressionContext ctx(compression_);
  UncompressionInfo info(ctx, *uncompression_dict_, compression_);
  Status s = Uncompress(info, src, buffer);
  return s;
}

template <typename RecordType>
Status BlobDecoder::DecodeRecord(Slice *src, RecordType *record,
                                 OwnedSlice *buffer) {
  TEST_SYNC_POINT_CALLBACK("BlobDecoder::DecodeRecord", &crc_);

  Slice input(src->data(), record_size_);
  src->remove_prefix(record_size_);

  Status s = CheckRecordCrc(input);
  if(!s.ok())
    return s;

  if (compression_ == kNoCompression) {
    return DecodeInto(input, record);
  }
  
  s = UncompressRecordIntoBuffer(input, buffer);
  if(!s.ok())
    return s;

  return DecodeInto(*buffer, record);
}

Status BlobDecoder::DecodeBlobRecord(Slice *src, BlobRecord *record,
                                     OwnedSlice *buffer) {
  return DecodeRecord<BlobRecord>(src, record, buffer);
}

Status BlobDecoder::ReadDeltaRecords(Slice *src, DeltaRecords *record,
                                     OwnedSlice *buffer) {
  return DecodeRecord<DeltaRecords>(src, record, buffer);
}

Status BlobDecoder::DecodeDeltaReocrds(Slice *src, BlobRecord *record,
                                       OwnedSlice *buffer,
                                       uint32_t delta_index) {
  DeltaRecords delta_records;
  Status s = ReadDeltaRecords(src, &delta_records, buffer);
  if (!s.ok())
    return s;
  s = DeltaUncompressDelta(delta_records, delta_index, record, buffer);
  return s;
}

Status BlobDecoder::DeltaUncompressDelta(const DeltaRecords &delta_records,
                                    uint32_t delta_index, BlobRecord *record,
                                    OwnedSlice *buffer) {
  Slice base = delta_records.value;
  Slice delta = delta_records.deltas_values[delta_index];
  Status s = DeltaUncompress(delta_compression_, delta, base, buffer);
  if (!s.ok())
    return s;
  record->key = delta_records.deltas_keys[delta_index];
  record->value = *buffer;
  return s;
}

void BlobHandle::EncodeTo(std::string* dst) const {
  PutVarint64(dst, offset);
  PutVarint64(dst, size);
}

Status BlobHandle::DecodeFrom(Slice* src) {
  if (!GetVarint64(src, &offset) || !GetVarint64(src, &size)) {
    return Status::Corruption("BlobHandle");
  }
  return Status::OK();
}

bool operator==(const BlobHandle& lhs, const BlobHandle& rhs) {
  return lhs.offset == rhs.offset && lhs.size == rhs.size;
}

void BlobIndex::EncodeTo(std::string* dst) const {
  dst->push_back(type);
  PutVarint64(dst, file_number);
  blob_handle.EncodeTo(dst);
}

inline bool BlobIndex::GoodType() {
  return type == kBlobRecord || type == kDeltaRecords;
}

Status BlobIndex::DecodeFrom(Slice* src) {
  unsigned char read_type;
  if (!GetChar(src, &read_type) || !(GoodType()) ||
      !GetVarint64(src, &file_number)) {
    return Status::Corruption("BlobIndex");
  }
  type = static_cast<BlobType>(read_type);
  Status s = blob_handle.DecodeFrom(src);
  if (!s.ok()) {
    return Status::Corruption("BlobIndex", s.ToString());
  }
  return s;
}

void BlobIndex::EncodeDeletionMarkerTo(std::string* dst) {
  dst->push_back(kBlobRecord);  // The type doesn't matter, anyone is ok.
  PutVarint64(dst, 0);          // What matter is the 0 in the next line. This means the delete flag
  BlobHandle dummy;
  dummy.EncodeTo(dst);
}

bool BlobIndex::IsDeletionMarker(const BlobIndex& index) {
  return index.file_number == 0;
}

bool BlobIndex::operator==(const BlobIndex& rhs) const {
  return (file_number == rhs.file_number && blob_handle == rhs.blob_handle);
}

DeltaRecordsIndex::DeltaRecordsIndex(BlobIndex index) {
  type = BlobType::kDeltaRecords;
  file_number = index.file_number;
  blob_handle = index.blob_handle;
}

Status DeltaRecordsIndex::DecodeFromBehindBase(Slice *src) {
  if (!GetVarint32(src, &delta_index))
    return Status::Corruption("BlobDeltaRecordsIndex");
  return Status::OK();
}

void DeltaRecordsIndex::EncodeTo(std::string *dst) const {
    BlobIndex::EncodeTo(dst);
    PutVarint32(dst, delta_index);
  }

void MergeBlobIndex::EncodeTo(std::string* dst) const {
  BlobIndex::EncodeTo(dst);
  PutVarint64(dst, source_file_number);
  PutVarint64(dst, source_file_offset);
}

void MergeBlobIndex::EncodeToBase(std::string* dst) const {
  BlobIndex::EncodeTo(dst);
}

Status MergeBlobIndex::DecodeFrom(Slice* src) {
  Status s = BlobIndex::DecodeFrom(src);
  if (!s.ok()) {
    return s;
  }
  if (!GetVarint64(src, &source_file_number) ||
      !GetVarint64(src, &source_file_offset)) {
    return Status::Corruption("MergeBlobIndex");
  }
  return s;
}

Status MergeBlobIndex::DecodeFromBase(Slice* src) {
  return BlobIndex::DecodeFrom(src);
}

bool MergeBlobIndex::operator==(const MergeBlobIndex& rhs) const {
  return (source_file_number == rhs.source_file_number &&
          source_file_offset == rhs.source_file_offset &&
          BlobIndex::operator==(rhs));
}

void BlobFileMeta::EncodeTo(std::string* dst) const {
  PutVarint64(dst, file_number_);
  PutVarint64(dst, file_size_);
  PutVarint64(dst, file_entries_);
  PutVarint32(dst, file_level_);
  PutLengthPrefixedSlice(dst, smallest_key_);
  PutLengthPrefixedSlice(dst, largest_key_);
}

Status BlobFileMeta::DecodeFromLegacy(Slice* src) {
  if (!GetVarint64(src, &file_number_) || !GetVarint64(src, &file_size_)) {
    return Status::Corruption("BlobFileMeta decode legacy failed");
  }
  assert(smallest_key_.empty());
  assert(largest_key_.empty());
  return Status::OK();
}

Status BlobFileMeta::DecodeFrom(Slice* src) {
  if (!GetVarint64(src, &file_number_) || !GetVarint64(src, &file_size_) ||
      !GetVarint64(src, &file_entries_) || !GetVarint32(src, &file_level_)) {
    return Status::Corruption("BlobFileMeta decode failed");
  }
  Slice str;
  if (GetLengthPrefixedSlice(src, &str)) {
    smallest_key_.assign(str.data(), str.size());
  } else {
    return Status::Corruption("BlobSmallestKey Decode failed");
  }
  if (GetLengthPrefixedSlice(src, &str)) {
    largest_key_.assign(str.data(), str.size());
  } else {
    return Status::Corruption("BlobLargestKey decode failed");
  }
  return Status::OK();
}

bool operator==(const BlobFileMeta& lhs, const BlobFileMeta& rhs) {
  return (lhs.file_number_ == rhs.file_number_ &&
          lhs.file_size_ == rhs.file_size_ &&
          lhs.file_entries_ == rhs.file_entries_ &&
          lhs.file_level_ == rhs.file_level_);
}

void BlobFileMeta::FileStateTransit(const FileEvent& event) {
  switch (event) {
    case FileEvent::kFlushCompleted:
      // blob file maybe generated by flush or gc, because gc will rewrite valid
      // keys to memtable. If it's generated by gc, we will leave gc to change
      // its file state. If it's generated by flush, we need to change it to
      // normal state after flush completed.
      assert(state_ == FileState::kPendingLSM ||
             state_ == FileState::kPendingGC || state_ == FileState::kNormal ||
             state_ == FileState::kBeingGC || state_ == FileState::kObsolete);
      if (state_ == FileState::kPendingLSM) state_ = FileState::kNormal;
      break;
    case FileEvent::kGCCompleted:
      // file is marked obsoleted during gc
      if (state_ == FileState::kObsolete) {
        break;
      }
      assert(state_ == FileState::kPendingGC || state_ == FileState::kBeingGC);
      state_ = FileState::kNormal;
      break;
    case FileEvent::kCompactionCompleted:
      assert(state_ == FileState::kPendingLSM);
      state_ = FileState::kNormal;
      break;
    case FileEvent::kGCBegin:
      assert(state_ == FileState::kNormal);
      state_ = FileState::kBeingGC;
      break;
    case FileEvent::kGCOutput:
      assert(state_ == FileState::kInit);
      state_ = FileState::kPendingGC;
      break;
    case FileEvent::kFlushOrCompactionOutput:
      assert(state_ == FileState::kInit);
      state_ = FileState::kPendingLSM;
      break;
    case FileEvent::kDbRestart:
      assert(state_ == FileState::kInit);
      state_ = FileState::kNormal;
      break;
    case FileEvent::kDelete:
      assert(state_ != FileState::kObsolete);
      state_ = FileState::kObsolete;
      break;
    case FileEvent::kNeedMerge:
      if (state_ == FileState::kToMerge) {
        break;
      }
      assert(state_ == FileState::kNormal);
      state_ = FileState::kToMerge;
      break;
    case FileEvent::kReset:
      state_ = FileState::kNormal;
      break;
    default:
      assert(false);
  }
}

TitanInternalStats::StatsType BlobFileMeta::GetDiscardableRatioLevel() const {
  auto ratio = GetDiscardableRatio();
  TitanInternalStats::StatsType type;
  if (ratio < std::numeric_limits<double>::epsilon()) {
    type = TitanInternalStats::NUM_DISCARDABLE_RATIO_LE0;
  } else if (ratio <= 0.2) {
    type = TitanInternalStats::NUM_DISCARDABLE_RATIO_LE20;
  } else if (ratio <= 0.5) {
    type = TitanInternalStats::NUM_DISCARDABLE_RATIO_LE50;
  } else if (ratio <= 0.8) {
    type = TitanInternalStats::NUM_DISCARDABLE_RATIO_LE80;
  } else if (ratio <= 1.0 ||
             (ratio - 1.0) < std::numeric_limits<double>::epsilon()) {
    type = TitanInternalStats::NUM_DISCARDABLE_RATIO_LE100;
  } else {
    fprintf(stderr, "invalid discardable ratio  %lf for blob file %" PRIu64,
            ratio, this->file_number_);
    type = TitanInternalStats::NUM_DISCARDABLE_RATIO_LE100;
  }
  return type;
}

void BlobFileMeta::Dump(bool with_keys) const {
  fprintf(stdout, "file %" PRIu64 ", size %" PRIu64 ", level %" PRIu32,
          file_number_, file_size_, file_level_);
  if (with_keys) {
    fprintf(stdout, ", smallest key: %s, largest key: %s",
            Slice(smallest_key_).ToString(true /*hex*/).c_str(),
            Slice(largest_key_).ToString(true /*hex*/).c_str());
  }
  fprintf(stdout, "\n");
}

void BlobFileHeader::EncodeTo(std::string* dst) const {
  PutFixed32(dst, kHeaderMagicNumber);
  PutFixed32(dst, version);

  if (version == BlobFileHeader::kVersion2) {
    PutFixed32(dst, flags);
  }
}

Status BlobFileHeader::DecodeFrom(Slice* src) {
  uint32_t magic_number = 0;
  if (!GetFixed32(src, &magic_number) || magic_number != kHeaderMagicNumber) {
    return Status::Corruption(
        "Blob file header magic number missing or mismatched.");
  }
  if (!GetFixed32(src, &version) ||
      (version != kVersion1 && version != kVersion2)) {
    return Status::Corruption("Blob file header version missing or invalid.");
  }
  if (version == BlobFileHeader::kVersion2) {
    // Check that no other flags are set
    if (!GetFixed32(src, &flags) || flags & ~kHasUncompressionDictionary) {
      return Status::Corruption("Blob file header flags missing or invalid.");
    }
  }
  return Status::OK();
}

void BlobFileFooter::EncodeTo(std::string* dst) const {
  auto size = dst->size();
  meta_index_handle.EncodeTo(dst);
  // Add padding to make a fixed size footer.
  dst->resize(size + kEncodedLength - 12);
  PutFixed64(dst, kFooterMagicNumber);
  Slice encoded(dst->data() + size, dst->size() - size);
  PutFixed32(dst, crc32c::Value(encoded.data(), encoded.size()));
}

Status BlobFileFooter::DecodeFrom(Slice* src) {
  auto data = src->data();
  Status s = meta_index_handle.DecodeFrom(src);
  if (!s.ok()) {
    return Status::Corruption("BlobFileFooter", s.ToString());
  }
  // Remove padding.
  src->remove_prefix(data + kEncodedLength - 12 - src->data());
  uint64_t magic_number = 0;
  if (!GetFixed64(src, &magic_number) || magic_number != kFooterMagicNumber) {
    return Status::Corruption("BlobFileFooter", "magic number");
  }
  Slice decoded(data, src->data() - data);
  uint32_t checksum = 0;
  if (!GetFixed32(src, &checksum) ||
      crc32c::Value(decoded.data(), decoded.size()) != checksum) {
    return Status::Corruption("BlobFileFooter", "checksum");
  }
  return Status::OK();
}

bool operator==(const BlobFileFooter& lhs, const BlobFileFooter& rhs) {
  return (lhs.meta_index_handle.offset() == rhs.meta_index_handle.offset() &&
          lhs.meta_index_handle.size() == rhs.meta_index_handle.size());
}

}  // namespace titandb
}  // namespace rocksdb
