// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include "table/gear_block/gear_table_reader.h"

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "gear_table_coding.h"
#include "gear_table_index.h"
#include "memory/arena.h"
#include "monitoring/histogram.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/cache.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "table/block_based/block.h"
#include "table/block_based/filter_block.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/dynamic_bloom.h"
#include "util/hash.h"
#include "util/stop_watch.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Safely getting a uint32_t element from a char array, where, starting from
// `base`, every 4 bytes are considered as an fixed 32 bit integer.
inline uint32_t GetFixed32Element(const char* base, size_t offset) {
  return DecodeFixed32(base + offset * sizeof(uint32_t));
}
}  // namespace

// Iterator to iterate IndexedTable
class GearTableIterator : public InternalIterator {
 public:
  explicit GearTableIterator(GearTableReader* table, bool use_prefix_seek);
  // No copying allowed
  GearTableIterator(const GearTableIterator&) = delete;
  void operator=(const Iterator&) = delete;

  ~GearTableIterator() override;

  bool Valid() const override;

  void SeekToFirst() override;

  void SeekToLast() override;

  void Seek(const Slice& target) override;

  void SeekForPrev(const Slice& target) override;

  void Next() override;

  void Prev() override;

  Slice key() const override;

  Slice value() const override;

  Status status() const override;

 private:
  GearTableReader* table_;
  GearTableKeyDecoder decoder_;
  bool use_prefix_seek_;
  uint32_t offset_;
  uint32_t next_offset_;
  Slice key_;
  Slice value_;
  Status status_;
};

GearTableReader::GearTableReader(const ImmutableCFOptions& ioptions,
                                 std::unique_ptr<RandomAccessFileReader>&& file,
                                 const EnvOptions& storage_options,
                                 const InternalKeyComparator& icomparator,
                                 EncodingType encoding_type, uint64_t file_size,
                                 const TableProperties* table_properties,
                                 const SliceTransform* prefix_extractor)
    : internal_comparator_(icomparator),
      encoding_type_(encoding_type),
      full_scan_mode_(false),
      user_key_len_(static_cast<uint32_t>(table_properties->fixed_key_len)),
      prefix_extractor_(prefix_extractor),
      enable_bloom_(false),
      bloom_(6),
      file_info_(std::move(file), storage_options,
                 static_cast<uint32_t>(table_properties->data_size)),
      ioptions_(ioptions),
      file_size_(file_size),
      attached_index_file_size_(file_size),
      table_properties_(nullptr) {
  auto ori_file_name =
      file->file_name();  // it should be like
                          // /media/jinghuan/nvme/huawei_test//01234.sst
  std::string index_file_name =
      GearTableIndexBuilder::find_the_index_by_file_name(ioptions,
                                                         ori_file_name);
  std::unique_ptr<FSRandomAccessFile> index_file;
  uint64_t index_file_size;
  auto s = ioptions.fs->NewRandomAccessFile(
      index_file_name, FileOptions(storage_options), &index_file, nullptr);

  s = ioptions.fs->GetFileSize(index_file_name, IOOptions(), &index_file_size,
                               nullptr);
  assert(s.ok());
  std::unique_ptr<RandomAccessFileReader> index_file_reader(
      new RandomAccessFileReader(std::move(index_file), index_file_name));
  file_info_.attached_index_file = std::move(index_file_reader);
  if (attached_index_file_size_ == 0) {
    attached_index_file_size_ = index_file_size;
  }
}

extern const uint64_t kPlainTableMagicNumber = 0x8242229663bf9564ull;
extern const uint64_t kLegacyPlainTableMagicNumber = 0x4f3418eb7a8f13b8ull;

GearTableReader::~GearTableReader() {}

Status GearTableReader::Open(const ImmutableCFOptions& ioptions,
                             const EnvOptions& env_options,
                             const InternalKeyComparator& internal_comparator,
                             std::unique_ptr<RandomAccessFileReader>&& file,
                             uint64_t file_size,
                             std::unique_ptr<TableReader>* table_reader,
                             const bool immortal_table,
                             const SliceTransform* prefix_extractor) {
  if (file_size > GearTableIndex::kMaxFileSize) {
    return Status::NotSupported("File is too large for GearTableReader!");
  }

  TableProperties* props_ptr = nullptr;
  auto s = ReadTableProperties(file.get(), file_size, kPlainTableMagicNumber,
                               ioptions, &props_ptr,
                               true /* compression_type_missing */);
  std::shared_ptr<TableProperties> props(props_ptr);
  if (!s.ok()) {
    return s;
  }

  auto& user_props = props->user_collected_properties;
  auto prefix_extractor_in_file = props->prefix_extractor_name;

  EncodingType encoding_type = kPlain;
  auto encoding_type_prop =
      user_props.find(PlainTablePropertyNames::kEncodingType);
  if (encoding_type_prop != user_props.end()) {
    encoding_type = static_cast<EncodingType>(
        DecodeFixed32(encoding_type_prop->second.c_str()));
  }

  std::unique_ptr<GearTableReader> new_reader(new GearTableReader(
      ioptions, std::move(file), env_options, internal_comparator,
      encoding_type, file_size, props.get(), prefix_extractor));

  s = new_reader->MmapDataIfNeeded();
  if (!s.ok()) {
    return s;
  }

  new_reader->full_scan_mode_ = true;
  // PopulateIndex can add to the props, so don't store them until now
  new_reader->table_properties_ = props;

  if (immortal_table && new_reader->file_info_.is_mmap_mode) {
    new_reader->dummy_cleanable_.reset(new Cleanable());
  }

  *table_reader = std::move(new_reader);
  return s;
}

void GearTableReader::SetupForCompaction() {}

InternalIterator* GearTableReader::NewIterator(
    const ReadOptions& options, const SliceTransform* /* prefix_extractor */,
    Arena* arena, bool /*skip_filters*/, TableReaderCaller /*caller*/,
    size_t /*compaction_readahead_size*/, bool /* allow_unprepared_value */) {
  // Not necessarily used here, but make sure this has been initialized
  assert(table_properties_);

  // Auto prefix mode is not implemented in GearTable.
  //  bool use_prefix_seek = !IsTotalOrderMode() && !options.total_order_seek &&
  //                         !options.auto_prefix_mode;s
  bool use_prefix_seek = false;  // Geat table uses BTree and plain data mode
  // no prefix seek.
  if (arena == nullptr) {
    return new GearTableIterator(this, use_prefix_seek);
  } else {
    auto mem = arena->AllocateAligned(sizeof(GearTableIterator));
    return new (mem) GearTableIterator(this, use_prefix_seek);
  }
}

Status GearTableReader::PopulateIndexRecordList(
    GearTableIndexBuilder* index_builder,
    std::vector<uint32_t>* prefix_hashes) {
  Slice prev_key_prefix_slice;
  std::string prev_key_prefix_buf;
  uint32_t pos = data_start_offset_;

  bool is_first_record = true;
  Slice key_prefix_slice;
  GearTableKeyDecoder decoder(&file_info_, encoding_type_, user_key_len_,
                              prefix_extractor_);
  while (pos < file_info_.data_end_offset) {
    uint32_t key_offset = pos;
    ParsedInternalKey key;
    Slice value_slice;
    bool seekable = false;
    Status s = Next(&decoder, &pos, &key, nullptr, &value_slice, &seekable);
    if (!s.ok()) {
      return s;
    }

    key_prefix_slice = GetPrefix(key);
    if (enable_bloom_) {
      bloom_.AddHash(GetSliceHash(key.user_key));
    } else {
      if (is_first_record || prev_key_prefix_slice != key_prefix_slice) {
        if (!is_first_record) {
          prefix_hashes->push_back(GetSliceHash(prev_key_prefix_slice));
        }
        if (file_info_.is_mmap_mode) {
          prev_key_prefix_slice = key_prefix_slice;
        } else {
          prev_key_prefix_buf = key_prefix_slice.ToString();
          prev_key_prefix_slice = prev_key_prefix_buf;
        }
      }
    }

    index_builder->AddKeyPrefix(GetPrefix(key), key_offset);

    if (!seekable && is_first_record) {
      return Status::Corruption("Key for a prefix is not seekable");
    }

    is_first_record = false;
  }

  prefix_hashes->push_back(GetSliceHash(key_prefix_slice));
  auto s = index_.InitFromRawData(index_builder->Finish());
  return s;
}

Status GearTableReader::MmapDataIfNeeded() {
  if (file_info_.is_mmap_mode) {
    // Get mmapped memory.
    return file_info_.file->Read(IOOptions(), 0,
                                 static_cast<size_t>(file_size_),
                                 &file_info_.file_data, nullptr, nullptr);
  }
  return Status::OK();
}

// PopulateIndex() builds index of keys. It must be called before any query
// to the table.
//
// props: the table properties object that need to be stored. Ownership of
//        the object will be passed.
//
// TODO: check whether this functions is needed in our situation?
Status GearTableReader::PopulateIndex(TableProperties* props) {
  assert(props != nullptr);
  // This function used to generate the entire list of key indices.
  BlockContents index_block_contents;
  Status s =
      ReadMetaBlock(file_info_.attached_index_file.get(), nullptr,
                    attached_index_file_size_, kPlainTableMagicNumber,
                    ioptions_, GearTableIndexBuilder::kGearTableIndexBlock,
                    BlockType::kGearIndex, &index_block_contents, true);

  bool index_in_file = s.ok();
  assert(index_in_file);
  // this is not a bloom filter index, read it as the index block.
  Slice* index_block;

  index_block_alloc_ = std::move(index_block_contents.allocation);
  index_block = &index_block_contents.data;
  GearTableIndexBuilder indexBuilder(&arena_, ioptions_, prefix_extractor_);
  // TODO: read all index from the index file.
  props->user_collected_properties["gear_table_index_table_size"] =
      ToString(index_.GetIndexSize() * GearTableIndex::kOffsetLen);
  props->user_collected_properties["gear_table_index_table_size"] =
      ToString(index_.GetSubIndexSize());
  s = index_.InitFromRawData(*index_block);
  return s;
}

Status GearTableReader::GetOffset(GearTableKeyDecoder* decoder,
                                  const Slice& target, const Slice& prefix,
                                  uint32_t prefix_hash, bool& prefix_matched,
                                  uint32_t* offset) const {
  prefix_matched = false;
  uint32_t prefix_index_offset;
  auto res = index_.GetOffset(prefix_hash, &prefix_index_offset);
  if (res == GearTableIndex::kDirectToFile) {
    *offset = prefix_index_offset;
    return Status::OK();
  }

  // point to sub-index, need to do a binary search
  uint32_t upper_bound;
  const char* base_ptr =
      index_.GetSubIndexBasePtrAndUpperBound(prefix_index_offset, &upper_bound);
  uint32_t low = 0;
  uint32_t high = upper_bound;
  ParsedInternalKey mid_key;
  ParsedInternalKey parsed_target;
  if (!ParseInternalKey(target, &parsed_target)) {
    return Status::Corruption(Slice());
  }

  // The key is between [low, high). Do a binary search between it.
  while (high - low > 1) {
    uint32_t mid = (high + low) / 2;
    uint32_t file_offset = GetFixed32Element(base_ptr, mid);
    uint32_t tmp;
    Status s = decoder->NextKeyNoValue(file_offset, &mid_key, nullptr, &tmp);
    if (!s.ok()) {
      return s;
    }
    int cmp_result = internal_comparator_.Compare(mid_key, parsed_target);
    if (cmp_result < 0) {
      low = mid;
    } else {
      if (cmp_result == 0) {
        // Happen to have found the exact key or target is smaller than the
        // first key after base_offset.
        prefix_matched = true;
        *offset = file_offset;
        return Status::OK();
      } else {
        high = mid;
      }
    }
  }
  // Both of the key at the position low or low+1 could share the same
  // prefix as target. We need to rule out one of them to avoid to go
  // to the wrong prefix.
  ParsedInternalKey low_key;
  uint32_t tmp;
  uint32_t low_key_offset = GetFixed32Element(base_ptr, low);
  Status s = decoder->NextKeyNoValue(low_key_offset, &low_key, nullptr, &tmp);
  if (!s.ok()) {
    return s;
  }

  if (GetPrefix(low_key) == prefix) {
    prefix_matched = true;
    *offset = low_key_offset;
  } else if (low + 1 < upper_bound) {
    // There is possible a next prefix, return it
    prefix_matched = false;
    *offset = GetFixed32Element(base_ptr, low + 1);
  } else {
    // target is larger than a key of the last prefix in this bucket
    // but with a different prefix. Key does not exist.
    *offset = file_info_.data_end_offset;
  }
  return Status::OK();
}

Status GearTableReader::Next(GearTableKeyDecoder* decoder, uint32_t* offset,
                             ParsedInternalKey* parsed_key, Slice* internal_key,
                             Slice* value, bool* seekable) const {
  if (*offset == file_info_.data_end_offset) {
    *offset = file_info_.data_end_offset;
    return Status::OK();
  }

  if (*offset > file_info_.data_end_offset) {
    return Status::Corruption("Offset is out of file size");
  }

  uint32_t bytes_read;
  Status s = decoder->NextKey(*offset, parsed_key, internal_key, value,
                              &bytes_read, seekable);
  if (!s.ok()) {
    return s;
  }
  *offset = *offset + bytes_read;
  return Status::OK();
}

void GearTableReader::Prepare(const Slice& target) {
  if (enable_bloom_) {
    uint32_t prefix_hash = GetSliceHash(GetPrefix(target));
    bloom_.Prefetch(prefix_hash);
  }
}

Status GearTableReader::Get(const ReadOptions& /*ro*/, const Slice& target,
                            GetContext* get_context,
                            const SliceTransform* /* prefix_extractor */,
                            bool /*skip_filters*/) {
  // TODO: re-add the function Get()
  return Status::OK();
}

uint64_t GearTableReader::ApproximateOffsetOf(const Slice& /*key*/,
                                              TableReaderCaller /*caller*/) {
  return 0;
}

uint64_t GearTableReader::ApproximateSize(const Slice& /*start*/,
                                          const Slice& /*end*/,
                                          TableReaderCaller /*caller*/) {
  return 0;
}

GearTableIterator::GearTableIterator(GearTableReader* table,
                                     bool use_prefix_seek)
    : table_(table),
      decoder_(&table_->file_info_, table_->encoding_type_,
               table_->user_key_len_, table_->prefix_extractor_),
      use_prefix_seek_(use_prefix_seek) {
  next_offset_ = offset_ = table_->file_info_.data_end_offset;
}

GearTableIterator::~GearTableIterator() {}

bool GearTableIterator::Valid() const {
  return offset_ < table_->file_info_.data_end_offset &&
         offset_ >= table_->data_start_offset_;
}

void GearTableIterator::SeekToFirst() {
  status_ = Status::OK();
  next_offset_ = table_->data_start_offset_;
  if (next_offset_ >= table_->file_info_.data_end_offset) {
    next_offset_ = offset_ = table_->file_info_.data_end_offset;
  } else {
    Next();
  }
}

void GearTableIterator::SeekToLast() {
  assert(false);
  status_ = Status::NotSupported("SeekToLast() is not supported in GearTable");
  next_offset_ = offset_ = table_->file_info_.data_end_offset;
}

void GearTableIterator::Seek(const Slice& target) {
  if (use_prefix_seek_ != !table_->IsTotalOrderMode()) {
    // This check is done here instead of NewIterator() to permit creating an
    // iterator with total_order_seek = true even if we won't be able to Seek()
    // it. This is needed for compaction: it creates iterator with
    // total_order_seek = true but usually never does Seek() on it,
    // only SeekToFirst().
    status_ = Status::InvalidArgument(
        "total_order_seek not implemented for GearTable.");
    offset_ = next_offset_ = table_->file_info_.data_end_offset;
    return;
  }

  // If the user doesn't set prefix seek option and we are not able to do a
  // total Seek(). assert failure.
  if (table_->IsTotalOrderMode()) {
    if (table_->full_scan_mode_) {
      status_ =
          Status::InvalidArgument("Seek() is not allowed in full scan mode.");
      offset_ = next_offset_ = table_->file_info_.data_end_offset;
      return;
    } else if (table_->GetIndexSize() > 1) {
      assert(false);
      status_ = Status::NotSupported(
          "GearTable cannot issue non-prefix seek unless in total order "
          "mode.");
      offset_ = next_offset_ = table_->file_info_.data_end_offset;
      return;
    }
  }

  Slice prefix_slice = table_->GetPrefix(target);
  uint32_t prefix_hash = 0;
  // Bloom filter is ignored in total-order mode.

  bool prefix_match;
  status_ = table_->GetOffset(&decoder_, target, prefix_slice, prefix_hash,
                              prefix_match, &next_offset_);
  if (!status_.ok()) {
    offset_ = next_offset_ = table_->file_info_.data_end_offset;
    return;
  }

  if (next_offset_ < table_->file_info_.data_end_offset) {
    for (Next(); status_.ok() && Valid(); Next()) {
      if (!prefix_match) {
        // Need to verify the first key's prefix
        if (table_->GetPrefix(key()) != prefix_slice) {
          offset_ = next_offset_ = table_->file_info_.data_end_offset;
          break;
        }
        prefix_match = true;
      }
      if (table_->internal_comparator_.Compare(key(), target) >= 0) {
        break;
      }
    }
  } else {
    offset_ = table_->file_info_.data_end_offset;
  }
}

void GearTableIterator::SeekForPrev(const Slice& /*target*/) {
  assert(false);
  status_ = Status::NotSupported("SeekForPrev() is not supported in GearTable");
  offset_ = next_offset_ = table_->file_info_.data_end_offset;
}

void GearTableIterator::Next() {
  offset_ = next_offset_;
  if (offset_ < table_->file_info_.data_end_offset) {
    Slice tmp_slice;
    ParsedInternalKey parsed_key;
    status_ =
        table_->Next(&decoder_, &next_offset_, &parsed_key, &key_, &value_);
    if (!status_.ok()) {
      offset_ = next_offset_ = table_->file_info_.data_end_offset;
    }
  }
}

void GearTableIterator::Prev() { assert(false); }

Slice GearTableIterator::key() const {
  assert(Valid());
  return key_;
}

Slice GearTableIterator::value() const {
  assert(Valid());
  return value_;
}

Status GearTableIterator::status() const { return status_; }

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
