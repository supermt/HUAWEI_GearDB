//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "table/gear_block/gear_table_coding.h"

#include <algorithm>
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
Status GearTableFileReader::NextKey(uint32_t offset,
                                    ParsedInternalKey* parsedKey,
                                    Slice* internalKey, Slice* value,
                                    uint32_t* bytes_read, bool* seekable) {
  return Status();
}
Status GearTableFileReader::NextBlock(uint32_t offset) {
  return Status(); }

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LIT
