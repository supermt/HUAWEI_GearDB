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

namespace {

// a utility that helps writing block content to the file
//   @offset will advance if @block_contents was successfully written.
//   @block_handle the block handle this particular block.
IOStatus WriteBlock(const Slice& block_contents, WritableFileWriter* file,
                    uint64_t* offset, BlockHandle* block_handle) {
  block_handle->set_offset(*offset);
  block_handle->set_size(block_contents.size());
  IOStatus io_s = file->Append(block_contents);

  if (io_s.ok()) {
    *offset += block_contents.size();
  }
  return io_s;
}

}  // namespace

extern const uint64_t kPlainTableMagicNumber = 0x8242229663bf9564ull;
extern const uint64_t kLegacyPlainTableMagicNumber = 0x4f3418eb7a8f13b8ull;

GearTableBuilder::GearTableBuilder(
    const ImmutableCFOptions& ioptions, const MutableCFOptions& moptions,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, WritableFileWriter* file, uint32_t user_key_len,
    uint32_t user_value_len, EncodingType encoding_type,
    const std::string& column_family_name, int target_level)
    : ioptions_(ioptions),
      moptions_(moptions),
      file_(file),
      offset_(0),
      current_key_length(0),
      current_value_length(0),
      encoder_(encoding_type, user_key_len, moptions.prefix_extractor.get()),
      prefix_extractor_(moptions.prefix_extractor.get()) {
  std::string ori_file_name = file->file_name();
  std::string index_file_name =
      GearTableIndexBuilder::find_the_index_by_file_name(ioptions,
                                                         ori_file_name);
  // Build index block and save it in the file if hash_table_ratio > 0
  index_builder_.reset(
      new GearTableIndexBuilder(&arena_, ioptions, index_file_name));
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
  PutFixed32(&val, static_cast<uint32_t>(encoder_.GetEncodingType()));
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
// | Header 64bit, (data_block_num+ entry_count) | value array...key array|
void GearTableBuilder::FlushDataBlock() {
  // add another key would extends the data block limit
  properties_.num_data_blocks += 1;
  PutFixed32(&block_header_buffer, properties_.num_data_blocks);
  PutFixed32(&block_header_buffer, page_entry_count);
  // Gear table don't need any meta data.
  std::reverse(block_key_buffer.begin(), block_key_buffer.end());
  std::string data_block =
      block_header_buffer + block_value_buffer + block_key_buffer;
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
  // temp buffer for metadata bytes between key and value.
  char meta_bytes_buf[6];
  size_t meta_bytes_buf_size = 0;

  ParsedInternalKey internal_key;
  if (!ParseInternalKey(key, &internal_key)) {
    assert(false);
    return;
  }
  if (internal_key.type == kTypeRangeDeletion) {
    status_ = Status::NotSupported("Range deletion unsupported");
    return;
  }

  if (current_key_length == 0) {
    // empty block buffer
    block_header_buffer.clear();
    block_key_buffer.clear();
    block_value_buffer.clear();
  }
  // place the entry into data blocks
  page_entry_count++;

  // we have two attributes in the header, and all offset are calculate from
  // the size of a char in the string.
  uint32_t value_offset = offset_ + CalculateHeaderSize();

  index_builder_->AddKeyOffset(internal_key.user_key, value_offset);
  block_key_buffer.append(key.data(), key.size());
  current_key_length = block_key_buffer.size();

  // Write value length
  uint32_t value_size = static_cast<uint32_t>(value.size());
  char* end_ptr =
      EncodeVarint32(meta_bytes_buf + meta_bytes_buf_size, value_size);
  assert(end_ptr <= meta_bytes_buf + sizeof(meta_bytes_buf));
  meta_bytes_buf_size = end_ptr - meta_bytes_buf;
  Slice value_meta_slice = Slice(meta_bytes_buf, meta_bytes_buf_size);

  block_value_buffer.append(value_meta_slice.data(), value_meta_slice.size());
  block_value_buffer.append(value.data(), value.size());
  current_value_length = block_value_buffer.size();

  if ((current_key_length + current_value_length + key.size() + value.size()) >
      data_block_size) {
    FlushDataBlock();
  }

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

  //  1. [meta block: bloom] - ignore
  //  2. [meta block: index] - ignore
  //  3. [meta block: properties]
  //  4. [metaindex block]
  //  5. [footer];

  PropertyBlockBuilder prop_block_builder;
  MetaIndexBuilder meta_index_builer;
  prop_block_builder.AddTableProperty(properties_);
  prop_block_builder.Add(properties_.user_collected_properties);
  NotifyCollectTableCollectorsOnFinish(table_properties_collectors_,
                                       ioptions_.info_log, &prop_block_builder);

  BlockHandle prop_block_handle;
  IOStatus s = WriteBlock(prop_block_builder.Finish(), file_, &offset_,
                          &prop_block_handle);

  if (!s.ok()) {
    return std::move(s);
  }
  meta_index_builer.Add(kPropertiesBlock, prop_block_handle);

  // -- write metaindex block
  BlockHandle metaindex_block_handle;
  io_status_ = WriteBlock(meta_index_builer.Finish(), file_, &offset_,
                          &metaindex_block_handle);
  if (!io_status_.ok()) {
    status_ = io_status_;
    return status_;
  }

  // Write Footer
  // no need to write out new footer if we're using default checksum
  Footer footer(kLegacyPlainTableMagicNumber, 0);
  footer.set_metaindex_handle(metaindex_block_handle);
  footer.set_index_handle(BlockHandle::NullBlockHandle());
  std::string footer_encoding;
  footer.EncodeTo(&footer_encoding);
  io_status_ = file_->Append(footer_encoding);
  if (io_status_.ok()) {
    offset_ += footer_encoding.size();
  }
  status_ = io_status_;

  // After this, call to the Finish in the index_builder
  index_builder_->Finish();

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
