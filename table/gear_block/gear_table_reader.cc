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
  explicit GearTableIterator(GearTableReader* table);
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
      index_(GearTableIndexReader::find_the_index_by_file_name(
          ioptions, file->file_name())),
      user_key_len_(static_cast<uint32_t>(table_properties->fixed_key_len)),
      prefix_extractor_(prefix_extractor),
      //      enable_bloom_(false),
      //      bloom_(6),
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
      GearTableIndexReader::find_the_index_by_file_name(ioptions,
                                                        ori_file_name);
  //  file_info_.gear_index_file_name = index_file_name;
  //  std::unique_ptr<FSRandomAccessFile> index_file;
  //  uint64_t index_file_size;
  //  auto s = ioptions.fs->NewRandomAccessFile(
  //      index_file_name, FileOptions(storage_options), &index_file, nullptr);
  //
  //  s = ioptions.fs->GetFileSize(index_file_name, IOOptions(),
  //  &index_file_size,
  //                               nullptr);
  //  assert(s.ok());
  //  std::unique_ptr<RandomAccessFileReader> index_file_reader(
  //      new RandomAccessFileReader(std::move(index_file), index_file_name));
  //  file_info_.attached_index_file = std::move(index_file_reader);
  //  if (attached_index_file_size_ == 0) {
  //    attached_index_file_size_ = index_file_size;
  //  }
}

const uint64_t kPlainTableMagicNumber = 0x8242229663bf9564ull;
const uint64_t kLegacyPlainTableMagicNumber = 0x4f3418eb7a8f13b8ull;

GearTableReader::~GearTableReader() {}

Status GearTableReader::Open(const ImmutableCFOptions& ioptions,
                             const EnvOptions& env_options,
                             const InternalKeyComparator& internal_comparator,
                             std::unique_ptr<RandomAccessFileReader>&& file,
                             uint64_t file_size,
                             std::unique_ptr<TableReader>* table_reader,
                             const bool immortal_table,
                             const SliceTransform* prefix_extractor) {
  if (file_size > GearTableIndexReader::kMaxFileSize) {
    return Status::NotSupported(
        "Origin File is too large for GearTableReader!");
  }

  TableProperties* props_ptr = nullptr;

  // for now, gear table won't compress the data block
  auto s = ReadTableProperties(file.get(), file_size, kPlainTableMagicNumber,
                               ioptions, &props_ptr,
                               true /* compression_type_missing */);

  std::shared_ptr<TableProperties> props(props_ptr);
  if (!s.ok()) {
    return s;
  }

  // a string to string mapping.
  auto& user_props = props->user_collected_properties;
  // this should be empty
  auto prefix_extractor_in_file = props->prefix_extractor_name;
  assert(prefix_extractor_in_file == nullptr);

  EncodingType encoding_type = kPlain;
  auto encoding_type_prop =
      user_props.find(PlainTablePropertyNames::kEncodingType);
  // if found
  if (encoding_type_prop != user_props.end()) {
    encoding_type = static_cast<EncodingType>(
        DecodeFixed32(encoding_type_prop->second.c_str()));
  }
  // not found, set to default value: kPlain.

  std::unique_ptr<GearTableReader> new_reader(new GearTableReader(
      ioptions, std::move(file), env_options, internal_comparator,
      encoding_type, file_size, props.get(), prefix_extractor));

  // Plain table and gear table are both random accessed file, use Mmap if
  // needed.
  s = new_reader->MmapDataIfNeeded();
  if (!s.ok()) {
    return s;
  }
  // generate the index for table, since we won't do the full scan, but for
  // current implementation there is no index generation part.

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
  //                         !options.auto_prefix_mode;
  //  bool use_prefix_seek = false;
  // Geat table uses BTree and plain data mode no prefix seek.
  if (arena == nullptr) {
    return new GearTableIterator(this);
  } else {
    auto mem = arena->AllocateAligned(sizeof(GearTableIterator));
    return new (mem) GearTableIterator(this);
  }
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

Status GearTableReader::GetOffset(GearTableKeyDecoder* decoder,
                                  const Slice& target, uint32_t* offset) const {
  // the target is the search target, we don't need the prefix
  auto search_result = index_.GetOffset(target, offset);
  if (search_result == GearTableIndexReader::kNotFound) {
    // according to the plain table, it's not possible to enter this status
    ParsedInternalKey parsed_target;
    if (!ParseInternalKey(target, &parsed_target)) {
      return Status::Corruption(Slice());
    }
    search_result = index_.GetOffset(parsed_target.user_key, offset);
    if (search_result == GearTableIndexReader::kDirectToFile) {
      // still found
      return Status::OK();
    }
    return Status::NotFound();
    // it's also caused by false parsing.
  } else {
    return Status::OK();
  }
  // we have the btree as the index, so we don't need the binary search.
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
  // No need for calculating the prefix
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

GearTableIterator::GearTableIterator(GearTableReader* table)
    : table_(table),
      decoder_(&table_->file_info_, table_->encoding_type_,
               table_->user_key_len_, table_->prefix_extractor_) {
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
  // don't need to check the scan mode
  status_ = table_->GetOffset(&decoder_, target, &next_offset_);
  if (!status_.ok()) {
    offset_ = next_offset_ = table_->file_info_.data_end_offset;
    return;
  }
  if (next_offset_ < table_->file_info_.data_end_offset) {
    // target not founded, but no errors.
    for (Next(); status_.ok() && Valid(); Next()) {
      // search forward
      if (table_->internal_comparator_.Compare(key(), target) >= 0) {
        // not founded
        break;
      }
    }
  } else {
    offset_ = table_->file_info_.data_end_offset;
  }
  // the table reader will search through the index
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
