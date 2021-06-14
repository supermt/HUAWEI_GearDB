//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <rocksdb/io_status.h>

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "memory/arena.h"
#include "monitoring/histogram.h"
#include "options/cf_options.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

class GearTableIndex {
 public:
  enum IndexSearchResult { kNotFound = 0, kDirectToFile = 1 };

  explicit GearTableIndex(Slice data) { InitFromRawData(data); }

  GearTableIndex() : index_size_(0), {}

  // Search through the B-Tree to get the value location
  IndexSearchResult GetOffset(uint32_t prefix_hash,
                              uint32_t* bucket_value) const;

  // Initialize data from `index_data`, which points to raw data for
  // index stored in the SST file.
  Status InitFromRawData(Slice index_data);

  const char* GetSubIndexBasePtrAndUpperBound(uint32_t offset,
                                              uint32_t* upper_bound) const {
    const char* index_ptr = &sub_index_[offset];
    return GetVarint32Ptr(index_ptr, index_ptr + 4, upper_bound);
  }

  uint32_t GetIndexSize() const { return index_size_; }

  uint32_t GetSubIndexSize() const { return sub_index_size_; }

  uint32_t GetNumPrefixes() const { return num_prefixes_; }

  static const uint64_t kMaxFileSize = (1u << 31) - 1;
  static const uint32_t kSubIndexMask = 0x80000000;
  static const size_t kOffsetLen = sizeof(uint32_t);

 private:
  uint32_t index_size_;
  uint32_t sub_index_size_;
  uint32_t num_prefixes_;

  uint32_t* index_;
  char* sub_index_;
};

class GearTableIndexBuilder {
 public:
  GearTableIndexBuilder(Arena* arena, const ImmutableCFOptions& ioptions,
                        const std::string index_file_name)
      : arena_(arena), ioptions_(ioptions) {
    // TODO: create the writable file according to the index_file_name
  }
  static std::string find_the_index_by_file_name(
      const ImmutableCFOptions& ioptions, std::string& ori_file_name) {
    assert(ioptions.db_paths.size() != 0);
    std::string index_dir =
        ioptions.db_paths[0].path + ioptions.index_dir_prefix;
    std::string delimiter = "/";

    size_t pos = 0;
    std::string token;
    while ((pos = ori_file_name.find(delimiter)) != std::string::npos) {
      token = ori_file_name.substr(0, pos);
      ori_file_name.erase(0, pos + delimiter.length());
    }
    return (index_dir + token);
  }
  void AddKeyPrefix(Slice key_prefix_slice, uint32_t key_offset);

  void AddKeyOffset(Slice key, uint32_t key_offset);

  IOStatus Finish();

  uint32_t GetTotalSize() const {
    return VarintLength(index_size_) + VarintLength(num_prefixes_) +
           GearTableIndex::kOffsetLen * index_size_ + sub_index_size_;
  }

  static const std::string kGearTableIndexBlock;

 private:
  struct IndexRecord {
    uint32_t hash;    // hash of the prefix
    uint32_t offset;  // offset of a row
    IndexRecord* next;
  };

  // Helper class to track all the index records
  class IndexRecordList {
   public:
    explicit IndexRecordList(size_t num_records_per_group)
        : kNumRecordsPerGroup(num_records_per_group),
          current_group_(nullptr),
          num_records_in_current_group_(num_records_per_group) {}

    ~IndexRecordList() {
      for (size_t i = 0; i < groups_.size(); i++) {
        delete[] groups_[i];
      }
    }

    void AddRecord(uint32_t hash, uint32_t offset);

    size_t GetNumRecords() const {
      return (groups_.size() - 1) * kNumRecordsPerGroup +
             num_records_in_current_group_;
    }
    IndexRecord* At(size_t index) {
      return &(
          groups_[index / kNumRecordsPerGroup][index % kNumRecordsPerGroup]);
    }

   private:
    IndexRecord* AllocateNewGroup() {
      IndexRecord* result = new IndexRecord[kNumRecordsPerGroup];
      groups_.push_back(result);
      return result;
    }

    // Each group in `groups_` contains fix-sized records (determined by
    // kNumRecordsPerGroup). Which can help us minimize the cost if resizing
    // occurs.
    const size_t kNumRecordsPerGroup;
    IndexRecord* current_group_;
    // List of arrays allocated
    std::vector<IndexRecord*> groups_;
    size_t num_records_in_current_group_;
  };

  void AllocateIndex();

  // Internal helper function to bucket index record list to hash buckets.
  void BucketizeIndexes(std::vector<IndexRecord*>* hash_to_offsets,
                        std::vector<uint32_t>* entries_per_bucket);

  // Internal helper class to fill the indexes and bloom filters to internal
  // data structures.
  Slice FillIndexes(const std::vector<IndexRecord*>& hash_to_offsets,
                    const std::vector<uint32_t>& entries_per_bucket);

  Arena* arena_;
  const ImmutableCFOptions ioptions_;
  WritableFileWriter* file_;

  const SliceTransform* prefix_extractor_;

  std::string prev_key_prefix_;

  static const size_t kRecordsPerGroup = 256;
};

};  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
