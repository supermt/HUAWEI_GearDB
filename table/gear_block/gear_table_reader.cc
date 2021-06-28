// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include "table/gear_block/gear_table_reader.h"

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "gear_table_file_reader.h"
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
  GearTableFileReader* file_reader_;
  uint64_t visited_key_counts_;
  uint64_t total_entry_count;
  // we don't need the next_key, since it's just the visited + 1;
  //  uint32_t offset_;
  //  uint32_t next_offset_;
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
  file_reader_ =
      new GearTableFileReader(internal_comparator_, &file_info_, file_size);
  entry_counts_in_file_ = file_reader_->GetEntryCount();
  assert(entry_counts_in_file_ > 0);
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

Status GearTableReader::GetOffset(const Slice& target, uint32_t* offset) const {
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

Status GearTableReader::Next(uint32_t* offset, ParsedInternalKey* parsed_key,
                             Slice* internal_key, Slice* value,
                             bool* seekable) const {
  if (*offset == file_info_.data_end_offset) {
    *offset = file_info_.data_end_offset;
    return Status::OK();
  }

  if (*offset > file_info_.data_end_offset) {
    return Status::Corruption("Offset is out of file size");
  }

  uint32_t bytes_read;
  Status s = file_reader_->NextKey(*offset, parsed_key, internal_key, value,
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
    : file_reader_(table->file_reader_), visited_key_counts_(0) {
  total_entry_count = file_reader_->GetEntryCount();
}

GearTableIterator::~GearTableIterator() {}

bool GearTableIterator::Valid() const {
  return visited_key_counts_ < total_entry_count &&
         visited_key_counts_ >= file_reader_->EntryCountStartPosition();
}

void GearTableIterator::SeekToFirst() {
  status_ = Status::OK();
  visited_key_counts_ = file_reader_->EntryCountStartPosition();
  Next();
}

void GearTableIterator::SeekToLast() {
  //  assert(false);
  //  status_ = Status::NotSupported("SeekToLast() is not supported in
  //  GearTable");
  // actually, we support this kind of operations.
  visited_key_counts_ = total_entry_count - 1;
  // save one a position for the last call to Next()
  Next();
  // we seek to the last position of the key array.
}

void GearTableIterator::Seek(const Slice& target) {
  // don't need to check the scan mode
  // for the iterator, we don't need the index, we use the file itself.

  // read the current key first.
  ParsedInternalKey parsedKey;
  status_ =
      file_reader_->GetKey(visited_key_counts_, &parsedKey, &key_, &value_);
  if (!status_.ok()) {
    visited_key_counts_ = total_entry_count;
    return;
  }
  if (visited_key_counts_ < total_entry_count) {
    // target not founded, but no errors.
    for (Next(); status_.ok() && Valid(); Next()) {
      // search forward
      if (file_reader_->internal_comparator_.Compare(key(), target) >= 0) {
        // not founded
        break;
      }
    }
  } else {
    visited_key_counts_ = total_entry_count;
  }
  // the table reader will search through the index
}

void GearTableIterator::SeekForPrev(const Slice& /*target*/) {
  assert(false);
  status_ = Status::NotSupported("SeekForPrev() is not supported in GearTable");
}

void GearTableIterator::Next() {
  visited_key_counts_++;
  if (visited_key_counts_ < total_entry_count) {
    Slice tmp_slice;
    ParsedInternalKey parsed_key;
    status_ =
        file_reader_->GetKey(visited_key_counts_, &parsed_key, &key_, &value_);
    if (!status_.ok()) {
      visited_key_counts_ = total_entry_count;
    }
  }
}

void GearTableIterator::Prev() {
  visited_key_counts_--;
  if (visited_key_counts_ < file_reader_->EntryCountStartPosition()) {
    visited_key_counts_ = file_reader_->EntryCountStartPosition();
  }
}

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
