//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "table/gear_block/gear_table_builder.h"

#include <assert.h>

#include <limits>
#include <map>
#include <string>

#include "db/dbformat.h"
#include "file/writable_file_writer.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based/block_builder.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/plain/plain_table_builder.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

const uint64_t kPlainTableMagicNumber = 0x8242229663bf9564ull;
const uint64_t kLegacyPlainTableMagicNumber = 0x4f3418eb7a8f13b8ull;

GearTableBuilder::GearTableBuilder(
    const ImmutableCFOptions& ioptions, const MutableCFOptions& moptions,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, WritableFileWriter* file, uint32_t user_key_len,
    uint32_t /*user_value_len*/, EncodingType encoding_type,
    const std::string& column_family_name, int target_level
    //    WritableFileWriter* index_file
    )
    : ioptions_(ioptions),
      moptions_(moptions),
      file_(file),
      //      index_file_(index_file),

      offset_(0),
      current_key_length(0),
      current_value_length(0),
      status_(),
      io_status_(),
      page_entry_count(0),
      prefix_extractor_(moptions.prefix_extractor.get()) {
  std::string ori_file_name = file->file_name();

  index_builder_.reset(
      new GearTableIndexBuilder(&arena_, ioptions, ori_file_name));
  properties_
      .user_collected_properties[PlainTablePropertyNames::kBloomVersion] =
      "1";  // For future use

  properties_.fixed_key_len = user_key_len;

  this->estimate_size_limit_ =
      std::floor(std::pow(moptions.write_buffer_size, (target_level + 1)));
  // Initial from 0, add till the current size reaches estimate size limit
  properties_.num_data_blocks = 0;
  // Fill it later if store_index_in_file_ == true
  properties_.index_size = 0;
  properties_.filter_size = 0;
  properties_.format_version = (encoding_type == kPlain) ? 0 : 1;
  properties_.column_family_id = column_family_id;
  properties_.column_family_name = column_family_name;
  properties_.prefix_extractor_name = moptions_.prefix_extractor != nullptr
                                          ? moptions_.prefix_extractor->Name()
                                          : "nullptr";

  std::string val;
  PutFixed32(&val, static_cast<uint32_t>(kPlain));
  properties_
      .user_collected_properties[PlainTablePropertyNames::kEncodingType] = val;

  for (auto& collector_factories : *int_tbl_prop_collector_factories) {
    table_properties_collectors_.emplace_back(
        collector_factories->CreateIntTblPropCollector(column_family_id));
  }
}

GearTableBuilder::~GearTableBuilder() {}

// Here is the data format
// ------------------------------------------------------------------------
// | Header 32*4 *4 bit, (data_block_num+ entry_count + value_array_length +
// key_array_length) | value array |\0\0\0\0 |key array|
// notice that key array is in "World Hello" way, doesn't reverse the key
// content inside
void GearTableBuilder::FlushDataBlock() {
  // add another key would extends the data block limit
  properties_.num_data_blocks += 1;
  uint32_t placeholder_length = data_block_size - (block_header_buffer.size() +
                                                   block_value_buffer.size() +
                                                   block_header_buffer.size());

  PutFixed32(&block_header_buffer, properties_.num_data_blocks);
  PutFixed32(&block_header_buffer, page_entry_count);
  PutFixed32(&block_header_buffer, (uint32_t)block_value_buffer.size());
  PutFixed32(&block_header_buffer, (uint32_t)block_key_buffer.size());
  PutFixed32(&block_header_buffer, placeholder_length);
  // place holder
  PutFixed32(&block_header_buffer, page_entry_count);
  PutFixed32(&block_header_buffer, (uint32_t)block_value_buffer.size());
  PutFixed32(&block_header_buffer, (uint32_t)block_key_buffer.size());
  PutFixed32(&block_header_buffer, properties_.num_data_blocks);
  PutFixed32(&block_header_buffer, page_entry_count);
  PutFixed32(&block_header_buffer, (uint32_t)block_value_buffer.size());
  PutFixed32(&block_header_buffer, (uint32_t)block_key_buffer.size());
  PutFixed32(&block_header_buffer, properties_.num_data_blocks);
  PutFixed32(&block_header_buffer, page_entry_count);
  PutFixed32(&block_header_buffer, (uint32_t)block_value_buffer.size());
  PutFixed32(&block_header_buffer, (uint32_t)block_key_buffer.size());

  // reverse the strings again
  // Hello World -> olleH dlrodW -> World Hello
  std::reverse(block_key_buffer.begin(), block_key_buffer.end());
  std::string data_block = block_header_buffer + block_value_buffer +
                           std::string(placeholder_length, 0x0) +
                           block_key_buffer;
  io_status_ = file_->Append(data_block);
  offset_ += data_block.size();
  // after flushing, reset the pointer
  current_value_length = 0;
  current_key_length = 0;
  page_entry_count = 0;
  block_key_buffer.clear();
  block_value_buffer.clear();
  block_header_buffer.clear();
}
void GearTableBuilder::Add(const Slice& key, const Slice& value) {
  //  if (current_key_length == 0) {
  //    // empty block buffer
  //    block_header_buffer.clear();
  //    block_key_buffer.clear();
  //    block_value_buffer.clear();
  //  }

  // if the length tends to exceed block size, flush first
  if ((current_key_length + current_value_length + key.size() + value.size()) >
      data_block_size) {
    FlushDataBlock();
  }
  // place the entry into data blocks
  page_entry_count++;

  // handle the keys
  ParsedInternalKey internal_key;
  if (!ParseInternalKey(key, &internal_key)) {
    assert(false);
    return;
  }
  if (internal_key.type == kTypeRangeDeletion) {
    status_ = Status::NotSupported("Range deletion unsupported");
    return;
  }
  std::string key_str = key.ToString();
  std::reverse(key_str.begin(), key_str.end());
  // we have an 64-byte header, and this value offset is used for index only.
  block_key_buffer.append(key_str.data(), key_str.size());
  current_key_length = block_key_buffer.size();

  uint32_t value_offset = offset_ + CalculateHeaderSize();
  index_builder_->AddKeyOffset(internal_key.user_key, value_offset);

  if (kGearSaveValueMeta) {
    // temp buffer for metadata bytes between key and value.
    char meta_bytes_buf[6];
    size_t meta_bytes_buf_size = 0;
    // Write value length
    uint32_t value_size = static_cast<uint32_t>(value.size());
    char* end_ptr =
        EncodeVarint32(meta_bytes_buf + meta_bytes_buf_size, value_size);
    assert(end_ptr <= meta_bytes_buf + sizeof(meta_bytes_buf));
    meta_bytes_buf_size = end_ptr - meta_bytes_buf;
    Slice value_meta_slice = Slice(meta_bytes_buf, meta_bytes_buf_size);

    block_value_buffer.append(value_meta_slice.data(), value_meta_slice.size());
  }

  block_value_buffer.append(value.data(), value.size());
  current_value_length = block_value_buffer.size();

  if (io_status_.ok()) {
    properties_.num_entries++;
    properties_.raw_key_size += key.size();
    properties_.raw_value_size += value.size();
    if (internal_key.type == kTypeDeletion ||
        internal_key.type == kTypeSingleDeletion) {
      properties_.num_deletions++;
    } else if (internal_key.type == kTypeMerge) {
      properties_.num_merge_operands++;
    }
  }

  // notify property collectors
  NotifyCollectTableCollectorsOnAdd(
      key, value, offset_, table_properties_collectors_, ioptions_.info_log);
  status_ = io_status_;
}

Status GearTableBuilder::Finish() {
  assert(!closed_);

  // If there are blocks haven't been flushed, finish them.
  Info(ioptions_.info_log,
       "Finish block build, estimate write in bytes: %lu, actual write in "
       "bytes: %lu",
       estimate_size_limit_, offset_);
  if (current_key_length != 0 || current_value_length != 0) {
    FlushDataBlock();
  }

  closed_ = true;

  properties_.data_size = offset_;

  // Check if there is any remaining data.
  // record the following entries in the file as the meta data
  std::string meta_block = "";
  PutFixed64(&meta_block, properties_.num_data_blocks);
  PutFixed64(&meta_block, properties_.raw_key_size);
  PutFixed64(&meta_block, properties_.raw_value_size);
  PutFixed64(&meta_block, properties_.data_size);
  PutFixed64(&meta_block, properties_.index_size);
  PutFixed64(&meta_block, properties_.num_entries);
  PutFixed64(&meta_block, properties_.num_deletions);
  PutFixed64(&meta_block, properties_.num_merge_operands);
  PutFixed64(&meta_block, properties_.num_range_deletions);
  PutFixed64(&meta_block, properties_.format_version);
  PutFixed64(&meta_block, properties_.creation_time);
  PutFixed64(&meta_block, properties_.oldest_key_time);
  PutFixed64(&meta_block, properties_.file_creation_time);

  assert(meta_block.size() == GearTableFileReader::meta_page_size);
  io_status_ = file_->Append(meta_block);
  assert(io_status_.ok());
  //   After this, call to the Finish in the index_builder
  index_builder_->Finish();
  if (io_status_.ok()) {
    offset_ += meta_block.size();
  }
  status_ = io_status_;
  return status_;
}

void GearTableBuilder::Abandon() { closed_ = true; }

uint64_t GearTableBuilder::NumEntries() const {
  return properties_.num_entries;
}

uint64_t GearTableBuilder::FileSize() const { return offset_; }

std::string GearTableBuilder::GetFileChecksum() const {
  if (file_ != nullptr) {
    return file_->GetFileChecksum();
  } else {
    return kUnknownFileChecksum;
  }
}

const char* GearTableBuilder::GetFileChecksumFuncName() const {
  if (file_ != nullptr) {
    return file_->GetFileChecksumFuncName();
  } else {
    return kUnknownFileChecksumFuncName;
  }
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
