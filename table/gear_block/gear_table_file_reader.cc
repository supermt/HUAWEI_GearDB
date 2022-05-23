//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/gear_block/gear_table_file_reader.h"

#include <algorithm>
#include <iostream>
#include <string>

#include "db/dbformat.h"
#include "file/writable_file_writer.h"
#include "table/gear_block/gear_table_factory.h"
#include "table/gear_block/gear_table_reader.h"
namespace ROCKSDB_NAMESPACE {

enum GearTableEntryType : unsigned char {
  kFullKey = 0,
  kPrefixFromPreviousKey = 1,
  kKeySuffix = 2,
};
const uint64_t kPlainTableMagicNumber = 0x8242229663bf9564ull;
Slice GearTableFileReader::GetFromBuffer(Buffer* buffer, uint32_t file_offset,
                                         uint32_t len) {
  assert(file_offset + len <= file_info_->data_end_offset);
  return Slice(buffer->buf.get() + (file_offset - buffer->buf_start_offset),
               len);
}

bool GearTableFileReader::ReadNonMmap(uint32_t file_offset, uint32_t len,
                                      Slice* out) {
  const uint32_t kPrefetchSize = 256u;

  // Try to read from buffers.
  for (uint32_t i = 0; i < num_buf_; i++) {
    Buffer* buffer = buffers_[num_buf_ - 1 - i].get();
    if (file_offset >= buffer->buf_start_offset &&
        file_offset + len <= buffer->buf_start_offset + buffer->buf_len) {
      *out = GetFromBuffer(buffer, file_offset, len);
      return true;
    }
  }

  Buffer* new_buffer;
  // Data needed is not in any of the buffer. Allocate a new buffer.
  if (num_buf_ < buffers_.size()) {
    // Add a new buffer
    new_buffer = new Buffer();
    buffers_[num_buf_++].reset(new_buffer);
  } else {
    // Now simply replace the last buffer. Can improve the placement policy
    // if needed.
    new_buffer = buffers_[num_buf_ - 1].get();
  }

  assert(file_offset + len <= file_info_->data_end_offset);
  uint32_t size_to_read = std::min(file_info_->data_end_offset - file_offset,
                                   std::max(kPrefetchSize, len));
  if (size_to_read > new_buffer->buf_capacity) {
    new_buffer->buf.reset(new char[size_to_read]);
    new_buffer->buf_capacity = size_to_read;
    new_buffer->buf_len = 0;
  }
  Slice read_result;
  Status s =
      file_info_->file->Read(IOOptions(), file_offset, size_to_read,
                             &read_result, new_buffer->buf.get(), nullptr);
  if (!s.ok()) {
    status_ = s;
    return false;
  }
  new_buffer->buf_start_offset = file_offset;
  new_buffer->buf_len = size_to_read;
  *out = GetFromBuffer(new_buffer, file_offset, len);
  return true;
}

inline bool GearTableFileReader::ReadVarint32(uint32_t offset, uint32_t* out,
                                              uint32_t* bytes_read) {
  if (file_info_->is_mmap_mode) {
    const char* start = file_info_->file_data.data() + offset;
    const char* limit =
        file_info_->file_data.data() + file_info_->data_end_offset;
    const char* key_ptr = GetVarint32Ptr(start, limit, out);
    assert(key_ptr != nullptr);
    *bytes_read = static_cast<uint32_t>(key_ptr - start);
    return true;
  } else {
    return ReadVarint32NonMmap(offset, out, bytes_read);
  }
}

bool GearTableFileReader::ReadVarint32NonMmap(uint32_t offset, uint32_t* out,
                                              uint32_t* bytes_read) {
  const char* start;
  const char* limit;
  const uint32_t kMaxVarInt32Size = 6u;
  uint32_t bytes_to_read =
      std::min(file_info_->data_end_offset - offset, kMaxVarInt32Size);
  Slice bytes;
  if (!Read(offset, bytes_to_read, &bytes)) {
    return false;
  }
  start = bytes.data();
  limit = bytes.data() + bytes.size();

  const char* key_ptr = GetVarint32Ptr(start, limit, out);
  *bytes_read =
      (key_ptr != nullptr) ? static_cast<uint32_t>(key_ptr - start) : 0;
  return true;
}

void GearTableFileReader::DataPage::GenerateFromSlice(Slice* raw_data) {
  uint64_t offset = 0;  // skip the first 16 bytes, it's the meta
  uint64_t key_offset = 0;
  const int parsed_key_length = kGearTableFixedKeyLength + 8;

  std::string key_space =
      Slice(&raw_data->data()[value_array_length_ + placeholder_length_],
            key_array_length_)
          .ToString();

  //  std::reverse(reversed_data.begin(), reversed_data.end());
  key_data = Slice(key_space);
  Slice key_slice = Slice(key_space);
  ParsedInternalKey temp;
  key_array_.clear();
  value_array_.clear();
  for (uint32_t i = 0; i < entry_count_; i++) {
    Slice value;
    // read the value first.
    if (kGearSaveValueMeta) {
      uint32_t value_len;
      uint32_t vint32_length;
      ReadValueLen(raw_data, offset, &value_len, &vint32_length);
      offset += vint32_length;
      value = Slice(raw_data->data() + offset, value_len);
      offset += value_len;
    } else {
      value = Slice(raw_data->data() + offset, kGearTableFixedValueLength);
      offset += kGearTableFixedValueLength;
    }
    value_array_.push_back(value.ToString());
    Slice key = Slice(key_slice.data() + key_offset, parsed_key_length);
    // the key sequence is in opposite direction, use insert instead of push
    // back
    key_array_.insert(key_array_.begin(), key.ToString());
    key_offset += parsed_key_length;
  }
}

Status GearTableFileReader::NextBlock(uint32_t offset,
                                      uint32_t* data_block_size) {
  uint32_t header_fields[DATA_BLOCK_HEADER_SIZE / sizeof(uint32_t)] = {
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  uint32_t data_block_num, entry_count, value_array_length, key_array_length,
      placeholder_length_;
  Slice header_info_temp_result;
  Read(offset, header_field_num * sizeof(uint32_t), &header_info_temp_result);
  for (int i = 0; i < header_field_num; i++) {
    GetFixed32(&header_info_temp_result, &header_fields[i]);
  }
  data_block_num = header_fields[0];
  entry_count = header_fields[1];
  value_array_length = header_fields[2];
  key_array_length = header_fields[3];
  placeholder_length_ = header_fields[4];

  data_pages.data_page_list.emplace_back(data_block_num, entry_count,
                                         key_array_length, value_array_length);
  data_pages.data_page_offset.emplace_back(
      offset + header_field_num * sizeof(uint32_t),
      value_array_length + key_array_length);

  assert(DATA_BLOCK_HEADER_SIZE + key_array_length + value_array_length +
             placeholder_length_ ==
         PAGE_SIZE);
  *data_block_size = DATA_BLOCK_HEADER_SIZE + key_array_length +
                     value_array_length + placeholder_length_;
  //      value_array_length + key_array_length +header_field_num *
  //      sizeof(uint32_t);
  data_pages.total_data_pages++;
  return Status();
}

bool GearTableFileReader::Read(uint32_t file_offset, uint32_t len, Slice* out) {
  if (file_info_->is_mmap_mode) {
    assert(file_offset + len <= file_info_->data_end_offset);
    *out = Slice(file_info_->file_data.data() + file_offset, len);
    return true;
  } else {
    return ReadNonMmap(file_offset, len, out);
  }
}
Status GearTableFileReader::ReadMetaData() {
  Status s;
  Slice read_result;
  int file_offset = file_size_ - meta_page_size;
  int size_to_read = meta_page_size;
  char buffer[200];
  s = file_info_->file.get()->Read(IOOptions(), file_offset, size_to_read,
                                   &read_result, buffer, nullptr);
  assert(read_result.size() == meta_page_size);

  GetFixed64(&read_result, &(meta_infos.num_data_blocks));
  GetFixed64(&read_result, &(meta_infos.raw_key_size));
  GetFixed64(&read_result, &(meta_infos.raw_value_size));
  GetFixed64(&read_result, &(meta_infos.data_size));
  GetFixed64(&read_result, &(meta_infos.index_size));
  GetFixed64(&read_result, &(meta_infos.num_entries));
  GetFixed64(&read_result, &(meta_infos.num_deletions));
  GetFixed64(&read_result, &(meta_infos.num_merge_operands));
  GetFixed64(&read_result, &(meta_infos.num_range_deletions));
  GetFixed64(&read_result, &(meta_infos.format_version));
  GetFixed64(&read_result, &(meta_infos.creation_time));
  GetFixed64(&read_result, &(meta_infos.oldest_key_time));
  GetFixed64(&read_result, &(meta_infos.file_creation_time));

  return Status(s);
}
uint32_t GearTableFileReader::FromOffsetToBlockID(uint32_t offset) {
  for (unsigned long i = 0; i < data_pages.data_page_list.size(); i++) {
    if (data_pages.data_page_offset[i].first >= offset) return i;
  }
  return -1;
}

uint32_t GearTableFileReader::FromKeyIdToBlockID(uint64_t key_id,
                                                 uint32_t* in_blk_offset) {
  int default_page_size = data_pages.data_page_list[0].entry_count_;
  *in_blk_offset = key_id % default_page_size;
  return key_id / data_pages.data_page_list[0].entry_count_;
  //  uint32_t result = 0;
  //  uint64_t skipped_entries = 0;
  //  for (unsigned long i = 0; i < data_pages.data_page_list.size(); i++) {
  //    result = i;
  //    if (data_pages.data_page_list[i].entry_count_ + skipped_entries >
  //    key_id) {
  //      *in_blk_offset = key_id - skipped_entries;
  //      break;
  //    }
  //    skipped_entries += data_pages.data_page_list[i].entry_count_;
  //  }
  //  return result;
}

Status GearTableFileReader::GetKey(uint64_t key_id,
                                   ParsedInternalKey* parsedKey,
                                   Slice* internalKey, Slice* value) {
  assert(key_id < meta_infos.num_entries);
  uint32_t in_lbk_offset;
  uint32_t data_page_id = FromKeyIdToBlockID(key_id, &in_lbk_offset);
  bool blk_loaded = !data_pages.data_page_list[data_page_id].key_array_.empty();
  if (data_page_id > 0 && in_lbk_offset == 0) {
    // free the previous data page when we are accessing a key in new block
    data_pages.data_page_list[data_page_id - 1].FreeBuffer();
  }
  Status s;
  if (!blk_loaded) {
    LoadDataPage(data_page_id);
  }
  Slice temp_slice =
      Slice(data_pages.data_page_list[data_page_id].key_array_[in_lbk_offset]);

  *internalKey = temp_slice;
  if (!ParseInternalKey(*internalKey, parsedKey)) {
    return Status::Corruption(Slice("Corrected value while read the entry"));
  }

  *value = Slice(
      data_pages.data_page_list[data_page_id].value_array_[in_lbk_offset]);

  return Status::OK();
}
uint64_t GearTableFileReader::GetEntryCount() const {
  return meta_infos.num_entries;
}
Status GearTableFileReader::LoadDataPage(uint32_t blk_id) {
  auto offset_length_pair = data_pages.data_page_offset[blk_id];
  Slice raw_data;
  bool read_result =
      Read(offset_length_pair.first, offset_length_pair.second, &raw_data);
  if (!read_result) {
    assert(false);
    return Status::Corruption("data page fault");
  } else {
    data_pages.data_page_list[blk_id].GenerateFromSlice(&raw_data);
  }
  return Status::OK();
}
bool GearTableFileReader::ReadValueByOffset(uint32_t offset,
                                            const Slice& target_slice,
                                            ParsedInternalKey* full_ikey,
                                            Slice* value) {
  Slice user_key = ExtractUserKey(target_slice);
  auto blk_id = FromOffsetToBlockID(offset);
  // search through the data page
  if (data_pages.data_page_list[blk_id].key_array_.empty()) {
    LoadDataPage(blk_id);
  }  // remember to free the target entry

  int key_id = 0;
  bool founded = false;
  for (auto full_key : data_pages.data_page_list[blk_id].key_array_) {
    int compare_result =
        internal_comparator_.Compare(user_key, ExtractUserKey(full_key));
    if (compare_result >= 0) {
      // full_key is larger than the user_key
      founded = compare_result == 0;
      break;
    }
    key_id++;
  }
  ParseInternalKey(data_pages.data_page_list[blk_id].key_array_[key_id],
                   full_ikey);
  *value = data_pages.data_page_list[blk_id].value_array_[key_id];
  return founded;
}
Status GearTableFileReader::MmapDataIfNeeded() {
  if (file_info_->is_mmap_mode) {
    // Get mmapped memory.
    return file_info_->file->Read(IOOptions(), 0,
                                  static_cast<size_t>(file_size_),
                                  &file_info_->file_data, nullptr, nullptr);
  }
  return Status::OK();
}

bool GearTableFileReader::DataPage::ReadValueLen(Slice* raw_data,
                                                 uint32_t offset, uint32_t* out,
                                                 uint32_t* bytes_read) {
  const char* start;
  const char* limit;
  const uint32_t kMaxVarInt32size = 6u;
  uint32_t bytes_to_read =
      std::min((uint32_t)raw_data->size() - offset, kMaxVarInt32size);
  Slice bytes = Slice(raw_data->data() + offset, bytes_to_read);
  start = bytes.data();
  limit = bytes.data() + bytes.size();
  const char* key_ptr = GetVarint32Ptr(start, limit, out);
  *bytes_read =
      (key_ptr != nullptr) ? static_cast<uint32_t>(key_ptr - start) : 0;
  return true;
}
void GearTableFileReader::DataPage::FreeBuffer() {
  value_array_.clear();
  key_array_.clear();
}
}  // namespace ROCKSDB_NAMESPACE
