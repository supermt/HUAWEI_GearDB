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
  const static uint64_t single_compaction_size = 2l * 1204 * 1024 * 1024;
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
        force_compaction_(false) {}
  // function section
 public:
  Compaction* PickCompaction();

 private:
  const ImmutableCFOptions& ioptions_;
  const InternalKeyComparator* icmp_;
  double l0_score_;
  const std::string& cf_name_;
  const MutableCFOptions& mutable_cf_options_;
  VersionStorageInfo* vstorage_;
  GearCompactionPicker* picker_;
  LogBuffer* log_buffer_;
  const double first_l2_size_ratio_;
  const static uint64_t max_compaction_file_size = 2ul * 1024 * 1024 * 1024;

  // function section
 public:
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            int level) {
    // unlike the universal compaction, gear compaction still follows the
    // level-based rules.
    uint32_t p = 0;
    assert(!ioptions.cf_paths.empty());

    // size remaining in the most recent path
    uint64_t current_path_size = ioptions.cf_paths[0].target_size;

    uint64_t level_size;
    int cur_level = 0;

    // max_bytes_for_level_base denotes L1 size.
    // We estimate L0 size to be the same as L1.
    level_size = mutable_cf_options.max_bytes_for_level_base;

    // Last path is the fallback
    while (p < ioptions.cf_paths.size() - 1) {
      if (level_size <= current_path_size) {
        if (cur_level == level) {
          // Does desired level fit in this path?
          return p;
        } else {
          current_path_size -= level_size;
          if (cur_level > 0) {
            if (ioptions.level_compaction_dynamic_level_bytes) {
              // Currently, level_compaction_dynamic_level_bytes is ignored when
              // multiple db paths are specified. https://github.com/facebook/
              // rocksdb/blob/master/db/column_family.cc.
              // Still, adding this check to avoid accidentally using
              // max_bytes_for_level_multiplier_additional
              level_size = static_cast<uint64_t>(
                  level_size *
                  mutable_cf_options.max_bytes_for_level_multiplier);
            } else {
              level_size = static_cast<uint64_t>(
                  level_size *
                  mutable_cf_options.max_bytes_for_level_multiplier *
                  mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
            }
          }
          cur_level++;
          continue;
        }
      }
      p++;
      current_path_size = ioptions.cf_paths[p].target_size;
    }
    return p;
  }
  Compaction* PickCompactionLastLevel();
  Compaction* PickCompactionForLevel(int level);

  bool L2SmallTreeIsFilled() {
    auto l2_files = vstorage_->LevelFiles(vstorage_->num_levels() - 1);
    if (l2_files.size() <=
        pow(mutable_cf_options_.level0_file_num_compaction_trigger,
            vstorage_->num_levels() - 1)) {
      return false;
    }

    uint64_t l2_small_size_bytes = 0;
    for (auto&& l2_sst : l2_files) {
      if (l2_sst->get_l2_position() ==
          VersionStorageInfo::l2_small_tree_index) {
        l2_small_size_bytes += l2_sst->compensated_file_size;
      }
    }
    uint64_t total_db_bytes;
    for (int i = 0; i < vstorage_->num_levels(); i++) {
      total_db_bytes += vstorage_->NumLevelBytes(i);
    }
    if (static_cast<double>(l2_small_size_bytes) / total_db_bytes >
        first_l2_size_ratio_) {
      return true;
    }
    return false;
  }

  bool force_compaction_;
};

struct InputFileInfo {
  InputFileInfo() : f(nullptr), level(0), index(0) {}

  FileMetaData* f;
  size_t level;
  size_t index;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
