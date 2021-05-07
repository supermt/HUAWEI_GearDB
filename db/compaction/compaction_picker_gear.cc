//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker_gear.h"
#ifndef ROCKSDB_LITE

#include <cinttypes>
#include <limits>
#include <queue>
#include <string>
#include <utility>

#include "db/column_family.h"
#include "file/filename.h"
#include "logging/log_buffer.h"
#include "monitoring/statistics.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// determine whether the LSM need compaction or not.
// There are two general cases:
// When the upper level are fullfilled.
// Or, the first L2 file reaches 4% of total size.

bool GearCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  // Special Optimization for Original Rocksdb
  if (!vstorage->ExpiredTtlFiles().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
    return true;
  }
  if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }

  // For upper levels, use the level-based logic to do the compaction
  for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
    if (vstorage->CompactionScore(i) >= 1) {
      return true;
    }
  }

  int last_level = NumberLevels() - 1;

  // no matter the biggest file haven't created, do the compaction if the first
  // l2 file size is too large.
  return ((double)(vstorage->NumLevelBytes(last_level) -
                   vstorage->ComputeBiggestTreeSize()) >=
          first_l2_size_ratio * (double)(vstorage->EstimateLiveDataSize()));
}

namespace {
// A helper class that form universal compactions. The class is used by
// UniversalCompactionPicker::PickCompaction().
// The usage is to create the class, and get the compaction object by calling
// PickCompaction().

class GearCompactionBuilder {
  // differences from Origin Level compaction:
  // No IntraL0 Compaction
 public:
  GearCompactionBuilder(const std::string& cf_name,
                        VersionStorageInfo* vstorage,
                        CompactionPicker* compaction_picker,
                        LogBuffer* log_buffer,
                        const MutableCFOptions& mutable_cf_options,
                        const ImmutableCFOptions& ioptions,
                        double first_l2_size_ratio,
                        double upper_level_file_size_ratio)
      : cf_name_(cf_name),
        vstorage_(vstorage),
        compaction_picker_(compaction_picker),
        log_buffer_(log_buffer),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions),
        first_l2_size_ratio_(first_l2_size_ratio),
        upper_level_file_size_ratio_(upper_level_file_size_ratio) {
    //    level_compaction_trigger_num = std::vector<int>();
    for (int i = 0; i < ioptions.num_levels; i++) {
      // record the compaction trigger number
      int times = i - 0;
      int base = mutable_cf_options.level0_file_num_compaction_trigger;
      level_compaction_trigger_num.emplace_back(times * base);
    }
  }

  // Pick and return a compaction.
  Compaction* PickCompaction();

  Compaction* GetCompaction();
  void SetupInitialFiles();
  bool SetupOtherL0FilesIfNeeded();
  bool SetupBigTreeFiles();
  bool PickFileToCompact();
  void PickFileToCompact(
      const autovector<std::pair<int, FileMetaData*>>& level_files,
      bool compact_to_next_level);

  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;
  std::vector<int> level_compaction_trigger_num;
  int start_level_ = -1;
  int output_level_ = -1;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_manual_ = false;

  CompactionInputFiles start_level_inputs_;
  std::vector<CompactionInputFiles> compaction_inputs_;
  CompactionInputFiles output_level_inputs_;
  std::vector<FileMetaData*> grandparents_;
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableCFOptions& ioptions_;
  const double first_l2_size_ratio_;
  const double upper_level_file_size_ratio_;
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            int level) {
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
  }  // end GetPathId
  bool SetupOtherInputsIfNeeded();
};

void GearCompactionBuilder::PickFileToCompact(
    const autovector<std::pair<int, FileMetaData*>>& level_files,
    bool compact_to_next_level) {
  for (auto& level_file : level_files) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    start_level_ = level_file.first;
    if ((compact_to_next_level &&
         start_level_ == vstorage_->num_non_empty_levels() - 1) ||
        (start_level_ == 0 &&
         !compaction_picker_->level0_compactions_in_progress()->empty())) {
      continue;
    }
    if (compact_to_next_level) {
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
    } else {
      output_level_ = start_level_;
    }
    start_level_inputs_.files = {level_file.second};
    start_level_inputs_.level = start_level_;
    if (compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                   &start_level_inputs_)) {
      return;
    }
  }
  start_level_inputs_.files.clear();
}

bool GearCompactionBuilder::PickFileToCompact() {
  if (start_level_ == 0 &&
      !compaction_picker_->level0_compactions_in_progress()->empty()) {
    // level 0 is in progress, skip any other concurrent l0 compaction
    return false;
  }

  start_level_inputs_.clear();

  assert(start_level_ >= 0);

  // Pick the largest file in this level that is not already
  // being compacted
  const std::vector<int>& file_size =
      vstorage_->FilesByCompactionPri(start_level_);
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);

  unsigned int cmp_idx;
  for (cmp_idx = vstorage_->NextCompactionIndex(start_level_);
       cmp_idx < file_size.size(); cmp_idx++) {
    int index = file_size[cmp_idx];
    auto* f = level_files[index];

    // do not pick a file to compact if it is being compacted
    // from n-1 level.
    if (f->being_compacted) {
      continue;
    }

    start_level_inputs_.files.push_back(f);
    start_level_inputs_.level = start_level_;
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &start_level_inputs_) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {start_level_inputs_}, output_level_)) {
      // A locked (pending compaction) input-level file was pulled in due to
      // user-key overlap.
      start_level_inputs_.clear();
      continue;
    }

    // Now that input level is fully expanded, we check whether any output
    // files are locked due to pending compaction.
    //
    // Note we rely on ExpandInputsToCleanCut() to tell us whether any output-
    // level files are locked, not just the extra ones pulled in for user-key
    // overlap.
    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (!output_level_inputs.empty() &&
        !compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &output_level_inputs)) {
      start_level_inputs_.clear();
      continue;
    }
    base_index_ = index;
    break;
  }

  // store where to start the iteration in the next call to PickCompaction
  vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);

  return start_level_inputs_.size() > 0;
}

bool GearCompactionBuilder::SetupOtherL0FilesIfNeeded() {
  if (start_level_ == 0 && output_level_ != 0) {
    return compaction_picker_->GetOverlappingL0Files(
        vstorage_, &start_level_inputs_, output_level_, &parent_index_);
  }
  return true;
}

void GearCompactionBuilder::SetupInitialFiles() {
  // search through the levels to pick the file first.
  // Find the compactions by size on all levels.
  bool skipped_l0_to_base = false;
  for (int i = 0; i < compaction_picker_->NumberLevels() - 2; i++) {
    start_level_score_ = vstorage_->CompactionScore(i);
    start_level_ = vstorage_->CompactionScoreLevel(i);
    // assert the current level's compaction score is the biggest.
    assert(i == 0 || start_level_score_ <= vstorage_->CompactionScore(i - 1));
    if (start_level_score_ >= 1) {
      if (skipped_l0_to_base && start_level_ == vstorage_->base_level()) {
        // If L0->base_level compaction is pending, don't schedule further
        // compaction from base level. Otherwise L0->base_level compaction
        // may starve.
        continue;
      }
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
      if (PickFileToCompact()) {
        // found the compaction!
        if (start_level_ == 0) {
          // this is caused by a l0-l1 compaction
          compaction_reason_ = CompactionReason::kLevelL0FilesNum;
        } else {
          // this is caused by a fullfiled level larger than level-1
          compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
        }
        break;
      } else {
        // didn't find the compaction, clear the inputs
        start_level_inputs_.clear();
        if (start_level_ == 0) {
          skipped_l0_to_base = true;
        }
      }
    } else {
      // no need to search, since current score is the biggest, it's not caused
      // by compaction score
      break;
    }
  }
  if (!start_level_inputs_.empty()) {
    return;
  }

  // if we didn't find a compaction, check if there are any files marked for
  // compaction
  parent_index_ = base_index_ = -1;

  compaction_picker_->PickFilesMarkedForCompaction(
      cf_name_, vstorage_, &start_level_, &output_level_, &start_level_inputs_);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kFilesMarkedForCompaction;
    return;
  }

  // Bottommost Files Compaction on deleting tombstones
  PickFileToCompact(vstorage_->BottommostFilesMarkedForCompaction(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kBottommostFiles;
    return;
  }

  // TTL Compaction
  PickFileToCompact(vstorage_->ExpiredTtlFiles(), true);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kTtl;
    return;
  }

  // Periodic Compaction
  PickFileToCompact(vstorage_->FilesMarkedForPeriodicCompaction(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kPeriodicCompaction;
    return;
  }
}
bool GearCompactionBuilder::SetupOtherInputsIfNeeded() {
  // Setup input files from output level. For output to L0, we only compact
  // spans of files that do not interact with any pending compactions, so don't
  // need to consider other levels.
  if (output_level_ != 0) {
    output_level_inputs_.level = output_level_;
    if (!compaction_picker_->SetupOtherInputs(
            cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs_,
            &output_level_inputs_, &parent_index_, base_index_)) {
      return false;
    }

    compaction_inputs_.push_back(start_level_inputs_);
    if (!output_level_inputs_.empty()) {
      compaction_inputs_.push_back(output_level_inputs_);
    }

    // In some edge cases we could pick a compaction that will be compacting
    // a key range that overlap with another running compaction, and both
    // of them have the same output level. This could happen if
    // (1) we are running a non-exclusive manual compaction
    // (2) AddFile ingest a new file into the LSM tree
    // We need to disallow this from happening.
    if (compaction_picker_->FilesRangeOverlapWithCompaction(compaction_inputs_,
                                                            output_level_)) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      return false;
    }
    compaction_picker_->GetGrandparents(vstorage_, start_level_inputs_,
                                        output_level_inputs_, &grandparents_);
  } else {
    compaction_inputs_.push_back(start_level_inputs_);
  }
  return true;
}

Compaction* GearCompactionBuilder::PickCompaction() {
  SetupInitialFiles();
  if (start_level_inputs_.empty()) {
    return nullptr;
  }
  assert(start_level_ >= 0 && output_level_ >= 0);

  // search through the Score levels, if there is no other file, trigger L2
  // large compaction
  if (!SetupBigTreeFiles()) {
    return nullptr;
  }

  if (!SetupOtherL0FilesIfNeeded()) {
    // return nothing if the L0 files are not overlapped or being locked
    return nullptr;
  }

  // Pick files in the output level and expand more files in the start level
  // if needed.
  if (!SetupOtherInputsIfNeeded()) {
    return nullptr;
  }

  // Form a compaction object containing the files we picked.
  Compaction* c = GetCompaction();

  TEST_SYNC_POINT_CALLBACK("GearCompactionPicker::PickCompaction:Return", c);

  return c;
}

Compaction* GearCompactionBuilder::GetCompaction() {
  auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, std::move(compaction_inputs_),
      output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          ioptions_.compaction_style, vstorage_->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options_.max_compaction_bytes,
      GetPathId(ioptions_, mutable_cf_options_, output_level_),
      GetCompressionType(ioptions_, vstorage_, mutable_cf_options_,
                         output_level_, vstorage_->base_level()),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level_),
      /* max_subcompactions */ 0, std::move(grandparents_), is_manual_,
      start_level_score_, false /* deletion_compaction */, compaction_reason_);

  // If it's level 0 compaction, make sure we don't execute any other level 0
  // compactions in parallel
  compaction_picker_->RegisterCompaction(c);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  return c;
}
bool rocksdb::GearCompactionBuilder::SetupBigTreeFiles() {
  uint64_t live_data_size = vstorage_->EstimateLiveDataSize();
  uint64_t size_of_l2_small_tree = 0;
  uint64_t size_of_l2_big_tree = vstorage_->ComputeBiggestTreeSize();
  int last_level = compaction_picker_->NumberLevels();
  size_of_l2_small_tree =
      vstorage_->NumLevelBytes(compaction_picker_->NumberLevels() - 1) -
      size_of_l2_big_tree;

  if (static_cast<double>(size_of_l2_small_tree) >
      live_data_size * static_cast<double>(first_l2_size_ratio_)) {
    // small tree is too large, need to compact it with the larger tree
    autovector<std::pair<int, FileMetaData*>> all_l2_files;
    for (auto f : vstorage_->LevelFiles(last_level)) {
      // all l2 files should be compacted together.
      all_l2_files.emplace_back(last_level, f);
    }
    // this is the last level, no longer length to compact
    PickFileToCompact(all_l2_files, false);

    if (start_level_inputs_.empty()) {
      return false;
    } else {
      compaction_reason_ = CompactionReason::kGearCompactionAllInOne;
      return true;
    }
  }

  // no enough file to generate Bigtree
  return false;
}
// Used in universal compaction when trivial move is enabled.
// This structure is used for the construction of min heap
// that contains the file meta data, the level of the file
// and the index of the file in that level

struct InputFileInfo {
  InputFileInfo() : f(nullptr), level(0), index(0) {}

  FileMetaData* f;
  size_t level;
  size_t index;
};

}  // end namespace

Compaction* GearCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, LogBuffer* log_buffer,
    SequenceNumber earliest_memtable_seqno) {
  // should not trigger a compaction too early. Calculated by Sequence number.
  if ((earliest_memtable_seqno - vstorage->getOldest_snapshot_seqnum()) == 0) {
    return nullptr;
  }
  assert(earliest_memtable_seqno != vstorage->getOldest_snapshot_seqnum());
  GearCompactionBuilder builder(cf_name, vstorage, this, log_buffer,
                                mutable_cf_options, ioptions_,
                                first_l2_size_ratio, upper_level_size_ratio);
  return builder.PickCompaction();
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE
