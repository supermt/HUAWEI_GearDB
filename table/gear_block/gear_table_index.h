//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

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
  enum IndexSearchResult {
    kNoPrefixForBucket = 0,
    kDirectToFile = 1,
    kSubindex = 2
  };

  explicit GearTableIndex(Slice data) { InitFromRawData(data); }

  GearTableIndex()
      : index_size_(0),
        sub_index_size_(0),
        num_prefixes_(0),
        index_(nullptr),
        sub_index_(nullptr) {}

  // The function that executes the lookup the hash table.
  // The hash key is `prefix_hash`. The function fills the hash bucket
  // content in `bucket_value`, which is up to the caller to interpret.
  IndexSearchResult GetOffset(uint32_t prefix_hash,
                              uint32_t* bucket_value) const;

  // Initialize data from `index_data`, which points to raw data for
  // index stored in the SST file.
  Status InitFromRawData(Slice index_data);

  // Decode the sub index for specific hash bucket.
  // The `offset` is the value returned as `bucket_value` by GetOffset()
  // and is only valid when the return value is `kSubindex`.
  // The return value is the pointer to the starting address of the
  // sub-index. `upper_bound` is filled with the value indicating how many
  // entries the sub-index has.
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
                         const SliceTransform* prefix_extractor,
                         size_t index_sparseness, double hash_table_ratio,
                         size_t huge_page_tlb_size)
      : arena_(arena),
        ioptions_(ioptions),
        record_list_(kRecordsPerGroup),
        is_first_record_(true),
        due_index_(false),
        num_prefixes_(0),
        num_keys_per_prefix_(0),
        prev_key_prefix_hash_(0),
        index_sparseness_(index_sparseness),
        index_size_(0),
        sub_index_size_(0),
        prefix_extractor_(prefix_extractor),
        hash_table_ratio_(hash_table_ratio),
        huge_page_tlb_size_(huge_page_tlb_size) {}

  void AddKeyPrefix(Slice key_prefix_slice, uint32_t key_offset);

  Slice Finish();

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
      return &(groups_[index / kNumRecordsPerGroup]
      [index % kNumRecordsPerGroup]);
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
  HistogramImpl keys_per_prefix_hist_;
  IndexRecordList record_list_;
  bool is_first_record_;
  bool due_index_;
  uint32_t num_prefixes_;
  uint32_t num_keys_per_prefix_;

  uint32_t prev_key_prefix_hash_;
  size_t index_sparseness_;
  uint32_t index_size_;
  uint32_t sub_index_size_;

  const SliceTransform* prefix_extractor_;
  double hash_table_ratio_;
  size_t huge_page_tlb_size_;

  std::string prev_key_prefix_;

  static const size_t kRecordsPerGroup = 256;
};

};  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
