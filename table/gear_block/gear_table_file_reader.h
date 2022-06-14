//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <array>
#include <iostream>

#include "db/dbformat.h"
#include "rocksdb/slice.h"
#include "table/gear_block/gear_table_reader.h"

namespace ROCKSDB_NAMESPACE {

class WritableFile;
struct ParsedInternalKey;
struct GearTableReaderFileInfo;
enum GearTableEntryType : unsigned char;

struct GearTableReaderFileInfo {
  bool is_mmap_mode;
  Slice file_data;
  uint32_t data_end_offset;
  std::unique_ptr<RandomAccessFileReader> file;
  GearTableReaderFileInfo(std::unique_ptr<RandomAccessFileReader>&& _file,
                          const EnvOptions& storage_options,
                          uint32_t _data_size_offset)
      : is_mmap_mode(storage_options.use_mmap_reads),
        data_end_offset(_data_size_offset),
        file(std::move(_file)) {}
};

class GearTableFileReader {
 public:
  const static int DATA_BLOCK_HEADER_SIZE = kGearTableHeaderLength;
  const static uint32_t PAGE_SIZE = 8 * 1024u;
  const static int meta_page_size_info_entry_num = 10;
  const static int meta_page_time_info_entry_num = 3;
  const static int header_field_num = DATA_BLOCK_HEADER_SIZE / 4;
  const static int meta_page_size =
      (2 * meta_page_size_info_entry_num + 2 * meta_page_time_info_entry_num) *
      sizeof(uint32_t);

  explicit GearTableFileReader(const InternalKeyComparator& internal_comparator,
                               std::unique_ptr<RandomAccessFileReader>&& file,
                               const EnvOptions& storage_options,
                               const uint32_t data_size, uint64_t file_size)
      : internal_comparator_(internal_comparator),
        file_info_(new GearTableReaderFileInfo(std::move(file), storage_options,
                                               data_size)),
        num_buf_(0),
        file_size_(file_size) {
    this->ReadMetaData();
    uint64_t offset = 0;
    uint32_t current_block_size = 0;
    uint32_t scanned_entries = 0;
    while (scanned_entries < meta_infos.num_data_blocks) {
      NextBlock(offset, &current_block_size);
      scanned_entries++;
      if (current_block_size == 0) break;
      offset += current_block_size;
    }
  }
  bool Read(uint32_t file_offset, uint32_t len, Slice* out);
  void ReadAllDataBlock(std::string* output);
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

  //  Status NextKey(uint32_t offset, ParsedInternalKey* parsedKey,
  //                 Slice* internalKey, Slice* value, uint32_t* bytes_read,
  //                 bool* seekable);

  uint32_t FromKeyIdToBlockID(uint64_t key_id, uint32_t* in_blk_offset);
  Status LoadDataPage(uint32_t blk_id);
  Status GetKey(uint64_t key_id, ParsedInternalKey* parsedKey,
                Slice* internalKey, Slice* value);
  bool ReadValueByOffset(uint32_t offset, const Slice& target_slice,
                         ParsedInternalKey* full_ikey, Slice* value);
  uint32_t FromOffsetToBlockID(uint32_t offset);
  typedef Slice value_record;
  typedef Slice key_record;
  struct DataPage {
    void GenerateFromSlice(Slice* raw_data);
    DataPage()
        : data_block_num_(0), entry_count_(0), value_array_(0), key_array_(0) {}
    //    DataPage(const DataPage& source)
    //        : data_block_num_(source.data_block_num_),
    //          entry_count_(source.entry_count_),
    //          value_array_(source.value_array_),
    //          key_array_(source.key_array_) {}
    DataPage(uint32_t data_block_num, uint32_t entry_count,
             uint32_t key_array_length, uint32_t value_array_length)
        : data_block_num_(data_block_num),
          entry_count_(entry_count),
          value_array_(0),
          key_array_(0),
          key_array_length_(key_array_length),
          value_array_length_(value_array_length),
          placeholder_length_(GearTableFileReader::PAGE_SIZE -
                              value_array_length - key_array_length -
                              GearTableFileReader::DATA_BLOCK_HEADER_SIZE) {}
    void FreeBuffer();
    uint32_t data_block_num_;
    uint32_t entry_count_;
    std::vector<std::string> value_array_;
    std::vector<std::string> key_array_;
    uint32_t key_array_length_;
    uint32_t value_array_length_;
    uint32_t placeholder_length_;
    Slice key_data;
    static bool ReadValueLen(Slice* raw_data, uint32_t offset, uint32_t* out,
                             uint32_t* bytes_read);
  };
  struct DataPageContainer {
    DataPageContainer() : total_data_pages(0), data_page_list(0) {}
    uint64_t total_data_pages;
    std::vector<DataPage> data_page_list;
    std::vector<std::pair<uint32_t, uint32_t>> data_page_offset;
  };

  Status ReadMetaData();

  uint64_t GetEntryCount() const;
  static uint64_t EntryCountStartPosition() { return 0; }

  const InternalKeyComparator internal_comparator_;

  Status MmapDataIfNeeded();

 private:
  std::unique_ptr<GearTableReaderFileInfo> file_info_;
  struct Buffer {
    Buffer() : buf_start_offset(0), buf_len(0), buf_capacity(0) {}
    std::unique_ptr<char[]> buf;
    uint32_t buf_start_offset;
    uint32_t buf_len;
    uint32_t buf_capacity;
  };
  // this should only be used in the constructor.
  Status NextBlock(uint32_t offset, uint32_t* data_block_size);

  // Keep buffers for two recent reads.
  std::array<std::unique_ptr<Buffer>, 2> buffers_;
  uint32_t num_buf_;
  Status status_;
  TableProperties meta_infos;
  DataPageContainer data_pages;

  Slice GetFromBuffer(Buffer* buf, uint32_t file_offset, uint32_t len);
  uint64_t file_size_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
