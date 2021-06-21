//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <array>

#include "db/dbformat.h"
#include "rocksdb/slice.h"
#include "table/gear_block/gear_table_reader.h"

namespace ROCKSDB_NAMESPACE {

class WritableFile;
struct ParsedInternalKey;
struct GearTableReaderFileInfo;
enum GearTableEntryType : unsigned char;

class GearTableFileReader {
 public:
  explicit GearTableFileReader(const GearTableReaderFileInfo* _file_info)
      : file_info_(_file_info), num_buf_(0) {}
  bool Read(uint32_t file_offset, uint32_t len, Slice* out) {
    if (file_info_->is_mmap_mode) {
      assert(file_offset + len <= file_info_->data_end_offset);
      *out = Slice(file_info_->file_data.data() + file_offset, len);
      return true;
    } else {
      return ReadNonMmap(file_offset, len, out);
    }
  }

  const static uint32_t page_size = 4096;
  // If return false, status code is stored in status_.
  bool ReadNonMmap(uint32_t file_offset, uint32_t len, Slice* output);

  // *bytes_read = 0 means eof. false means failure and status is saved
  // in status_. Not directly returning Status to save copying status
  // object to map previous performance of mmap mode.
  inline bool ReadVarint32(uint32_t offset, uint32_t* output,
                           uint32_t* bytes_read);

  bool ReadVarint32NonMmap(uint32_t offset, uint32_t* output,
                           uint32_t* bytes_read);

  Status status() const { return status_; }

  const GearTableReaderFileInfo* file_info() { return file_info_; }

  Status NextKey(uint32_t offset, ParsedInternalKey* parsedKey,
                 Slice* internalKey, Slice* value, uint32_t* bytes_read,
                 bool* seekable);
  Status NextBlock(uint32_t offset);

 private:
  const GearTableReaderFileInfo* file_info_;

  struct Buffer {
    Buffer() : buf_start_offset(0), buf_len(0), buf_capacity(0) {}
    std::unique_ptr<char[]> buf;
    uint32_t buf_start_offset;
    uint32_t buf_len;
    uint32_t buf_capacity;
  };

  // Keep buffers for two recent reads.
  std::array<std::unique_ptr<Buffer>, 2> buffers_;
  uint32_t num_buf_;
  Status status_;

  Slice GetFromBuffer(Buffer* buf, uint32_t file_offset, uint32_t len);
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
