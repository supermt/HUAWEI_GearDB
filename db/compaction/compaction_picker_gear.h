//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#ifndef ROCKSDB_LITE

#include <queue>

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {

class GearCompactionPicker : public CompactionPicker {
 public:
  GearCompactionPicker(const ImmutableCFOptions& ioptions,
                       const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}

  // we have only three levels, each level has less than 10 files.
  // So we can use this vector to record the entire tree.
  // [ 0:[SST,SST,SST]
  // , 1:[IndexTree1,IndexTree2,IndexTree3]
  // , 2:[SmallTree, LargeTree] ]

  virtual Compaction* PickCompaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, LogBuffer* log_buffer,
      SequenceNumber earliest_memtable_seqno = kMaxSequenceNumber) override;

  virtual int MaxOutputLevel() const override {
    return NumberLevels() - 1;  // suppose to be 2 for now
  }                             // 3-1 = 2

  virtual bool NeedsCompaction(
      const VersionStorageInfo* vstorage) const override;

  int l1_file_compaction_trigger = 10;
  double first_l2_size_ratio =
      0.03;  // Size ratio of L2-1 file, when the size of this file exceed this
  // ratio, trigger a large merge
  double upper_level_size_ratio =
      0.04;  // Size ratio of L2-1 file, when the size of this file exceed this
  // ratio, trigger a large merge

};  // end class compaction gear picker

class GearCompactionBuilder {
  // differences from Origin Level compaction:
  // No IntraL0 Compaction
 public:
  GearCompactionBuilder(const ImmutableCFOptions& ioptions,
                        const InternalKeyComparator* icmp,
                        const std::string& cf_name,
                        const MutableCFOptions& mutable_cf_options,
                        VersionStorageInfo* vstorage,
                        GearCompactionPicker* picker, LogBuffer* log_buffer,
                        double first_l2_size_ratio,
                        double upper_level_file_size_ratio)
      : ioptions_(ioptions),
        icmp_(icmp),
        cf_name_(cf_name),
        mutable_cf_options_(mutable_cf_options),
        vstorage_(vstorage),
        picker_(picker),
        log_buffer_(log_buffer),
        first_l2_size_ratio_(first_l2_size_ratio),
        upper_level_file_size_ratio_(upper_level_file_size_ratio) {}
  // function section
 public:
  Compaction* PickCompaction();

 private:
  const ImmutableCFOptions& ioptions_;
  const InternalKeyComparator* icmp_;
  double score_;
  std::vector<VersionStorageInfo::IndexTree> sorted_runs_;
  const std::string& cf_name_;
  const MutableCFOptions& mutable_cf_options_;
  VersionStorageInfo* vstorage_;
  GearCompactionPicker* picker_;
  LogBuffer* log_buffer_;
  const double first_l2_size_ratio_;
  const double upper_level_file_size_ratio_;

  // function section
 public:
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            uint64_t file_size) {
    // Two conditions need to be satisfied:
    // (1) the target path needs to be able to hold the file's size
    // (2) Total size left in this and previous paths need to be not
    //     smaller than expected future file size before this new file is
    //     compacted, which is estimated based on size_ratio.
    // For example, if now we are compacting files of size (1, 1, 2, 4, 8),
    // we will make sure the target file, probably with size of 16, will be
    // placed in a path so that eventually when new files are generated and
    // compacted to (1, 1, 2, 4, 8, 16), all those files can be stored in or
    // before the path we chose.
    //
    // considered in this algorithm. So the target size can be violated in
    // that case. We need to improve it.
    uint64_t accumulated_size = 0;
    uint64_t future_size =
        file_size *
        (100 - mutable_cf_options.compaction_options_universal.size_ratio) /
        100;
    uint32_t p = 0;
    assert(!ioptions.cf_paths.empty());
    for (; p < ioptions.cf_paths.size() - 1; p++) {
      uint64_t target_size = ioptions.cf_paths[p].target_size;
      if (target_size > file_size &&
          accumulated_size + (target_size - file_size) > future_size) {
        return p;
      }
      accumulated_size += target_size;
    }
    return p;
  }
  Compaction* PickCompactionLastLevel();
  Compaction* PickCompactionToOldest(size_t start_index,
                                     CompactionReason compaction_reason);
  Compaction* PickDeleteTriggeredCompaction();
  bool IsInputFilesNonOverlapping(Compaction* c);
  Compaction* PickCompactionToReduceSortedRuns(
      unsigned int ratio, unsigned int max_number_of_files_to_compact);
  Compaction* PickCompactionForLevel(int level);
};

struct InputFileInfo {
  InputFileInfo() : f(nullptr), level(0), index(0) {}

  FileMetaData* f;
  size_t level;
  size_t index;
};

struct SmallestKeyHeapComparator {
  explicit SmallestKeyHeapComparator(const Comparator* ucmp) { ucmp_ = ucmp; }

  bool operator()(InputFileInfo i1, InputFileInfo i2) const {
    return (ucmp_->Compare(i1.f->smallest.user_key(),
                           i2.f->smallest.user_key()) > 0);
  }

 private:
  const Comparator* ucmp_;
};

typedef std::priority_queue<InputFileInfo, std::vector<InputFileInfo>,
                            SmallestKeyHeapComparator>
    SmallestKeyHeap;

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
