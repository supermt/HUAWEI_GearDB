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
#include <iostream>
#include <limits>
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
void IndexTree::Dump(char* out_buf, size_t out_buf_size,
                     bool print_path) const {
  if (level == 0) {
    assert(file != nullptr);
    if (file->fd.GetPathId() == 0 || !print_path) {
      snprintf(out_buf, out_buf_size, "file %" PRIu64, file->fd.GetNumber());
    } else {
      snprintf(out_buf, out_buf_size,
               "file %" PRIu64
               "(path "
               "%" PRIu32 ")",
               file->fd.GetNumber(), file->fd.GetPathId());
    }
  } else {
    snprintf(out_buf, out_buf_size, "level %d", level);
  }
}

void IndexTree::DumpSizeInfo(char* out_buf, size_t out_buf_size,
                             size_t sorted_run_count) const {
  if (level == 0) {
    assert(file != nullptr);
    snprintf(out_buf, out_buf_size,
             "file %" PRIu64 "[%" ROCKSDB_PRIszt
             "] "
             "with size %" PRIu64 " (compensated size %" PRIu64 ")",
             file->fd.GetNumber(), sorted_run_count, file->fd.GetFileSize(),
             file->compensated_file_size);
  } else {
    snprintf(out_buf, out_buf_size,
             "level %d[%" ROCKSDB_PRIszt
             "] "
             "with size %" PRIu64 " (compensated size %" PRIu64 ")",
             level, sorted_run_count, size, compensated_file_size);
  }
}
void GearCompactionBuilder::CalculateSortedRuns() {
  // Think this, in previous implementation, all index tree are calculated at
  // the beginning of PickCompaction. So, this index tree structure should not
  // be stored
  tree_level_map.clear();

  std::vector<IndexTree> temp;
  for (auto f : vstorage_->LevelFiles(0)) {
    temp.emplace_back(0, f, f->fd.GetFileSize(), f->compensated_file_size,
                      f->being_compacted);
  }
  tree_level_map.emplace_back(0, temp);
  for (int level = 1; level < vstorage_->num_levels() - 1; level++) {
    int file_num_limit =
        (int)pow(mutable_cf_options_.level0_file_num_compaction_trigger, level);
    temp.clear();

    uint64_t total_compensated_size = 0U;
    uint64_t total_size = 0U;
    bool being_compacted = false;
    IndexTree current_node(level, nullptr, 0, 0, false);
    for (FileMetaData* f : vstorage_->LevelFiles(level)) {
      if (current_node.AddFileToFdList(f, file_num_limit)) {
        current_node.size += f->fd.GetFileSize();
        current_node.compensated_file_size += f->compensated_file_size;
        if (f->being_compacted) {
          current_node.being_compacted = f->being_compacted;
        }
      } else {
        // the index tree is fulfilled.
        temp.emplace_back(level, nullptr, current_node.size,
                          current_node.compensated_file_size,
                          current_node.being_compacted, current_node.fd_list);

        current_node = IndexTree(level, nullptr, 0, 0, false);
        current_node.AddFileToFdList(f, file_num_limit);
        // still need to add the file
      }
    }
    tree_level_map.emplace_back(level, temp);
  }
  // Now we record all data inside the last level;
  int last_level = vstorage_->num_levels() - 1;
  temp.clear();
  // l2_position = 0, the small tree
  temp.emplace_back(last_level, nullptr, 0, 0, false);
  // l2_position = 1, the large tree
  temp.emplace_back(last_level, nullptr, 0, 0, false);
  // if l2_position = -1, abort, and -1 can't be used in array index, so ,there
  // is no need for assertion
  for (FileMetaData* f : vstorage_->LevelFiles(last_level)) {
    temp[f->l2_position].fd_list.push_back(f);
    temp[f->l2_position].size += f->fd.GetFileSize();
    temp[f->l2_position].compensated_file_size += f->compensated_file_size;
    if (f->being_compacted) {
      temp[f->l2_position].being_compacted = f->being_compacted;
    }
  }
  tree_level_map.emplace_back(last_level, temp);
}

void GearCompactionBuilder::getAllIndexTrees(std::vector<IndexTree>* results) {
  CalculateSortedRuns();
  for (auto level_files : tree_level_map) {
    for (auto tree : level_files.second) {
      results->emplace_back(tree);
    }
  }
}

bool GearCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  // precondition of universal compaction
  const int kLevel0 = 0;
  if (vstorage->CompactionScore(kLevel0) >= 1) {
    return true;
  }
  // remain compaction files
  if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  return false;
  // otherwise we will mark all files in the biggest tree as the one that needs
  // compaction
}

Compaction* GearCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, LogBuffer* log_buffer,
    SequenceNumber earliest_memtable_seqno) {
  // should not trigger a compaction too early. Calculated by Sequence number.
  if ((earliest_memtable_seqno - vstorage->getOldest_snapshot_seqnum()) == 0) {
    return nullptr;
  }
  assert(earliest_memtable_seqno != vstorage->getOldest_snapshot_seqnum());
  GearCompactionBuilder builder(ioptions_, icmp_, cf_name, mutable_cf_options,
                                vstorage, this, log_buffer, first_l2_size_ratio,
                                upper_level_size_ratio);
  return builder.PickCompaction();
}

SmallestKeyHeap create_level_heap(Compaction* c, const Comparator* ucmp) {
  SmallestKeyHeap smallest_key_priority_q =
      SmallestKeyHeap(SmallestKeyHeapComparator(ucmp));

  InputFileInfo input_file;

  for (size_t l = 0; l < c->num_input_levels(); l++) {
    if (c->num_input_files(l) != 0) {
      if (l == 0 && c->start_level() == 0) {
        for (size_t i = 0; i < c->num_input_files(0); i++) {
          input_file.f = c->input(0, i);
          input_file.level = 0;
          input_file.index = i;
          smallest_key_priority_q.push(std::move(input_file));
        }
      } else {
        input_file.f = c->input(l, 0);
        input_file.level = l;
        input_file.index = 0;
        smallest_key_priority_q.push(std::move(input_file));
      }
    }
  }
  return smallest_key_priority_q;
}

Compaction* GearCompactionBuilder::PickCompaction() {
  const int kLevel0 = 0;
  score_ = vstorage_->CompactionScore(kLevel0);
  sorted_runs_.clear();
  getAllIndexTrees(&sorted_runs_);

  if (sorted_runs_.empty() &&
      (vstorage_->FilesMarkedForPeriodicCompaction().empty() &&
       vstorage_->FilesMarkedForCompaction().empty()
       //       && sorted_runs.size() <
       //       mutable_cf_options_.level0_file_num_compaction_trigger
       // We would spend more time on ensuring there is nothing to compact
       )) {
    ROCKS_LOG_BUFFER(log_buffer_, "[%s] Gear: nothing to do\n",
                     cf_name_.c_str());
    return nullptr;
  }

  // record the LSM shape into the LOG file
  VersionStorageInfo::LevelSummaryStorage tmp;
  ROCKS_LOG_BUFFER_MAX_SZ(
      log_buffer_, 3072,
      "[%s] Gear Compaction: number of index trees: %" ROCKSDB_PRIszt
      " detailed info for SST files: %s\n",
      cf_name_.c_str(), sorted_runs_.size(), vstorage_->LevelSummary(&tmp));

  // We don't need the periodic compaction, instead, we need a compaction that
  // collect all data from L2. The determination condition is the size of L2
  // small tree is larger than a certain threshold.

  Compaction* c = nullptr;
  if (L2SmallTreeIsFilled() && !picker_->IsLevel0CompactionInProgress()) {
    // if the last level compaction is in progress, we still don't need to
    // collect any files.
    c = PickCompactionLastLevel();
  }

  // Find Compactions in the upper levels.
  if (c == nullptr) {
    // We skip the ReduceSizeAmp Compaction, and the read size ratio compaction

    // search through the levels from back to the front
    // L2 compaction's priority is always higher than L1
    // L1->L2's compaction's priority is also higher than L0
    int target_level = -1;
    for (int i = vstorage_->num_levels() - 2; i >= 0; i--) {
      if ((int)(tree_level_map[i].second.size()) >=
          mutable_cf_options_.level0_file_num_compaction_trigger) {
        for (auto& tree : tree_level_map[i].second) {
          if (tree.being_compacted) {
            continue;
          }
        }
        target_level = i;
        break;
      }
    }
    if (target_level == -1) {
      c = nullptr;
    } else {
      // different from origin design, we will collect an entire level to do
      // compact
      if ((c = PickCompactionForLevel(target_level)) != nullptr) {
        ROCKS_LOG_BUFFER(log_buffer_, "[%s] Gear: compacting for level %u\n",
                         cf_name_.c_str(), target_level);
      }
    }
  }

  // still, there is no compaction. Then we check if there is no compaction
  // But the delete compaction in gear compaction may be a little bit different
  // deleted it for now.
  //  if (c == nullptr) {
  //    if ((c = PickDeleteTriggeredCompaction()) != nullptr) {
  //      ROCKS_LOG_BUFFER(log_buffer_, "[%s] Gear: delete triggered
  //      compaction\n",
  //                       cf_name_.c_str());
  //    }
  //  }
  // Still no compaction founded, return
  if (c == nullptr) {
    return nullptr;
  }
  // if the compaction is not null, then try to find all NonOverLapping.
  // notice that this feature will only be triggered when the allow_trivial_move
  // options is triggered
  if (mutable_cf_options_.compaction_options_universal.allow_trivial_move &&
      (c->compaction_reason() != CompactionReason::kPeriodicCompaction ||
       c->compaction_reason() != CompactionReason::kGearCompactionAllInOne)) {
    c->set_is_trivial_move(IsInputFilesNonOverlapping(c));
  }

  // update statistics
  RecordInHistogram(ioptions_.statistics, NUM_FILES_IN_SINGLE_COMPACTION,
                    c->inputs(0)->size());

  picker_->RegisterCompaction(c);
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);

  return c;
}
bool GearCompactionBuilder::IsInputFilesNonOverlapping(Compaction* c) {
  auto comparator = icmp_->user_comparator();
  int first_iter = 1;

  InputFileInfo prev, curr, next;

  SmallestKeyHeap smallest_key_priority_q =
      create_level_heap(c, icmp_->user_comparator());

  while (!smallest_key_priority_q.empty()) {
    curr = smallest_key_priority_q.top();
    smallest_key_priority_q.pop();

    if (first_iter) {
      prev = curr;
      first_iter = 0;
    } else {
      if (comparator->Compare(prev.f->largest.user_key(),
                              curr.f->smallest.user_key()) >= 0) {
        // found overlapping files, return false
        return false;
      }
      assert(comparator->Compare(curr.f->largest.user_key(),
                                 prev.f->largest.user_key()) > 0);
      prev = curr;
    }

    next.f = nullptr;

    if (c->level(curr.level) != 0 &&
        curr.index < c->num_input_files(curr.level) - 1) {
      next.f = c->input(curr.level, curr.index + 1);
      next.level = curr.level;
      next.index = curr.index + 1;
    }

    if (next.f) {
      smallest_key_priority_q.push(std::move(next));
    }
  }
  return true;
}

Compaction* GearCompactionBuilder::PickDeleteTriggeredCompaction() {
  CompactionInputFiles start_level_inputs;
  int output_level;
  std::vector<CompactionInputFiles> inputs;

  if (vstorage_->num_levels() == 1) {
#if defined(ENABLE_SINGLE_LEVEL_DTC)
    // This is single level universal. Since we're basically trying to reclaim
    // space by processing files marked for compaction due to high tombstone
    // density, let's do the same thing as compaction to reduce size amp which
    // has the same goals.
    bool compact = false;

    start_level_inputs.level = 0;
    start_level_inputs.files.clear();
    output_level = 0;
    for (FileMetaData* f : vstorage_->LevelFiles(0)) {
      if (f->marked_for_compaction) {
        compact = true;
      }
      if (compact) {
        start_level_inputs.files.push_back(f);
      }
    }
    if (start_level_inputs.size() <= 1) {
      // If only the last file in L0 is marked for compaction, ignore it
      return nullptr;
    }
    inputs.push_back(start_level_inputs);
#else
    // Disable due to a known race condition.
    return nullptr;
#endif  // ENABLE_SINGLE_LEVEL_DTC
  } else {
    int start_level;

    // For multi-level universal, the strategy is to make this look more like
    // leveled. We pick one of the files marked for compaction and compact with
    // overlapping files in the adjacent level.
    picker_->PickFilesMarkedForCompaction(cf_name_, vstorage_, &start_level,
                                          &output_level, &start_level_inputs);
    if (start_level_inputs.empty()) {
      return nullptr;
    }

    // Pick the first non-empty level after the start_level
    for (output_level = start_level + 1; output_level < vstorage_->num_levels();
         output_level++) {
      if (vstorage_->NumLevelFiles(output_level) != 0) {
        break;
      }
    }

    // If all higher levels are empty, pick the highest level as output level
    if (output_level == vstorage_->num_levels()) {
      if (start_level == 0) {
        output_level = vstorage_->num_levels() - 1;
      } else {
        // If start level is non-zero and all higher levels are empty, this
        // compaction will translate into a trivial move. Since the idea is
        // to reclaim space and trivial move doesn't help with that, we
        // skip compaction in this case and return nullptr
        return nullptr;
      }
    }
    if (ioptions_.allow_ingest_behind &&
        output_level == vstorage_->num_levels() - 1) {
      assert(output_level > 1);
      output_level--;
    }

    if (output_level != 0) {
      if (start_level == 0) {
        if (!picker_->GetOverlappingL0Files(vstorage_, &start_level_inputs,
                                            output_level, nullptr)) {
          return nullptr;
        }
      }

      CompactionInputFiles output_level_inputs;
      int parent_index = -1;

      output_level_inputs.level = output_level;
      if (!picker_->SetupOtherInputs(cf_name_, mutable_cf_options_, vstorage_,
                                     &start_level_inputs, &output_level_inputs,
                                     &parent_index, -1)) {
        return nullptr;
      }
      inputs.push_back(start_level_inputs);
      if (!output_level_inputs.empty()) {
        inputs.push_back(output_level_inputs);
      }
      if (picker_->FilesRangeOverlapWithCompaction(inputs, output_level)) {
        return nullptr;
      }
    } else {
      inputs.push_back(start_level_inputs);
    }
  }

  uint64_t estimated_total_size = 0;
  // Use size of the output level as estimated file size
  for (FileMetaData* f : vstorage_->LevelFiles(output_level)) {
    estimated_total_size += f->fd.GetFileSize();
  }
  uint32_t path_id = GetPathId(ioptions_, mutable_cf_options_, output_level);
  return new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, std::move(inputs),
      output_level,
      MaxFileSizeForLevel(mutable_cf_options_, output_level,
                          kCompactionStyleGear),
      /* max_grandparent_overlap_bytes */ LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage_, mutable_cf_options_,
                         output_level, 1),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level),
      /* max_subcompactions */ 0, /* grandparents */ {}, /* is manual */ false,
      score_, false /* deletion_compaction */,
      CompactionReason::kFilesMarkedForCompaction);
}

Compaction* GearCompactionBuilder::PickCompactionToOldest(
    size_t start_index, CompactionReason compaction_reason) {
  assert(start_index < sorted_runs_.size());

  int start_level = sorted_runs_[start_index].level;

  std::vector<CompactionInputFiles> inputs(vstorage_->num_levels());
  for (size_t i = 0; i < inputs.size(); i++) {
    inputs[i].level = i;  // here we should not modify the level.
  }

  for (size_t loop = start_index; loop < sorted_runs_.size(); loop++) {
    auto& picking_sr = sorted_runs_[loop];
    if (picking_sr.level == 0) {
      FileMetaData* f = picking_sr.file;
      inputs[0].files.push_back(f);
    } else {
      for (auto* f : picking_sr.fd_list) {
        inputs[picking_sr.level].files.push_back(f);
      }
    }

    std::string comp_reason_print_string;
    if (compaction_reason == CompactionReason::kPeriodicCompaction) {
      comp_reason_print_string = "periodic compaction";
    } else if (compaction_reason ==
               CompactionReason::kUniversalSizeAmplification) {
      comp_reason_print_string = "size amp";
    } else if (compaction_reason == CompactionReason::kGearCompactionAllInOne) {
      comp_reason_print_string = "compaction l2 in one large tree";
    } else {
      assert(false);
    }

    char file_num_buf[256];
    picking_sr.DumpSizeInfo(file_num_buf, sizeof(file_num_buf), loop);
    ROCKS_LOG_BUFFER(log_buffer_, "[%s] Gear : %s picking %s", cf_name_.c_str(),
                     comp_reason_print_string.c_str(), file_num_buf);
  }

  // output files at the bottom most level, unless it's reserved
  int output_level = vstorage_->num_levels() - 1;
  // last level is reserved for the files ingested behind
  if (ioptions_.allow_ingest_behind) {
    // by default, it's not allowed.
    assert(output_level > 1);
    output_level--;
  }

  uint32_t path_id = GetPathId(ioptions_, mutable_cf_options_, output_level);
  // We never check size for
  // compaction_options_universal.compression_size_percent,
  // because we always compact all the files, so always compress.
  return new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, std::move(inputs),
      output_level,
      MaxFileSizeForLevel(mutable_cf_options_, output_level,
                          kCompactionStyleUniversal),
      LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage_, mutable_cf_options_, start_level,
                         1, true),
      GetCompressionOptions(mutable_cf_options_, vstorage_, start_level, true),
      0, {}, false, score_, false, compaction_reason);
}

Compaction* GearCompactionBuilder::PickCompactionForLevel(int level) {
  auto& level_trees = tree_level_map[level].second;
  assert((int)level_trees.size() >=
         mutable_cf_options_.level0_file_num_compaction_trigger);

  std::vector<IndexTree> candidates;
  for (const auto& f : level_trees) {
    if (!f.being_compacted) candidates.push_back(f);
  }

  if (candidates.size() <
      mutable_cf_options_.compaction_options_universal.min_merge_width) {
    return nullptr;
  }

  std::vector<CompactionInputFiles> inputs(vstorage_->num_levels());
  for (size_t i = 0; i < inputs.size(); ++i) {
    inputs[i].level = 0 + static_cast<int>(i);
  }
  int loop = 0;
  for (auto& picking_sr : candidates) {
    if (picking_sr.level == 0) {
      FileMetaData* f = picking_sr.file;
      inputs[0].files.push_back(f);
    } else {
      for (auto* f : picking_sr.fd_list) {
        inputs[picking_sr.level].files.push_back(f);
      }
    }
    char file_num_buf[256];
    picking_sr.DumpSizeInfo(file_num_buf, sizeof(file_num_buf), loop);
//    ROCKS_LOG_BUFFER(log_buffer_, "[%s] Gear : %s picking %s", cf_name_.c_str(),
//                     "merge upper level files", file_num_buf);
    loop++;
  }
  int start_level = level;
  int output_level = level + 1;

  if (!picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                       &inputs[start_level]) ||
      picker_->FilesRangeOverlapWithCompaction(inputs, output_level)) {
    // A locked (pending compaction) input-level file was pulled in due to
    // user-key overlap.
    return nullptr;
  }
  autovector<std::pair<int, FileMetaData*>> level_files;
  if (start_level == 0 && !picker_->IsLevel0CompactionInProgress()) {
    // try to collect as many l0 files as possible.
    picker_->GetOverlappingL0Files(vstorage_, &inputs[0], output_level,
                                   nullptr);
  }
  // use the compaction_picker to collect the range
  InternalKey left_key, right_key;
  picker_->GetRange(inputs, &left_key, &right_key);
  // use the version storage info to search the overlapping files.
  // changed here, we won't create a new output_level input, instead,
  // we use the target level files in the inputs.
  CompactionInputFiles* output_level_inputs = &inputs[output_level];
  //  output_level_inputs.level = output_level;
  vstorage_->GetOverlappingInputs(output_level, &left_key, &right_key,
                                  &output_level_inputs->files);
  bool overlapped_upper_files = false;
  bool overlapped_files_cleared = true;
  if (!output_level_inputs->empty()) {
    overlapped_upper_files = true;
    overlapped_files_cleared = picker_->ExpandInputsToCleanCut(
        cf_name_, vstorage_, output_level_inputs);
  }

  if (overlapped_upper_files && !overlapped_files_cleared) {
    // if overlapped with upper files, we collect more files
    return nullptr;
  }

  bool overlapped_with_compactions =
      picker_->FilesRangeOverlapWithCompaction(inputs, output_level);

  if (overlapped_with_compactions) {
    return nullptr;
  }

  uint32_t path_id = GetPathId(ioptions_, mutable_cf_options_, output_level);

  return new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, std::move(inputs),
      output_level,
      MaxFileSizeForLevel(mutable_cf_options_, output_level,
                          kCompactionStyleGear),
      mutable_cf_options_.max_compaction_bytes, path_id,
      GetCompressionType(ioptions_, vstorage_, mutable_cf_options_, start_level,
                         vstorage_->base_level(), true),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level), 0,
      {}, false, score_, false, CompactionReason::kGearCollectTiered);
}

Compaction* GearCompactionBuilder::PickCompactionToReduceSortedRuns(
    unsigned int ratio, unsigned int max_number_of_files_to_compact) {
  unsigned int min_merge_width =
      mutable_cf_options_.compaction_options_universal.min_merge_width;
  unsigned int max_merge_width =
      mutable_cf_options_.compaction_options_universal.max_merge_width;

  const IndexTree* sr = nullptr;
  bool done = false;
  size_t start_index = 0;
  unsigned int candidate_count = 0;

  unsigned int max_files_to_compact =
      std::min(max_merge_width, max_number_of_files_to_compact);
  min_merge_width = std::max(min_merge_width, 2U);

  // Caller checks the size before executing this function. This invariant is
  // important because otherwise we may have a possible integer underflow when
  // dealing with unsigned types.
  assert(!sorted_runs_.empty());

  // Considers a candidate file only if it is smaller than the
  // total size accumulated so far.
  for (size_t loop = 0; loop < sorted_runs_.size(); loop++) {
    candidate_count = 0;

    // Skip files that are already being compacted
    for (sr = nullptr; loop < sorted_runs_.size(); loop++) {
      sr = &sorted_runs_[loop];

      if (!sr->being_compacted) {
        candidate_count = 1;
        break;
      }
      char file_num_buf[kFormatFileNumberBufSize];
      sr->Dump(file_num_buf, sizeof(file_num_buf));
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] Gear: %s"
                       "[%d] being compacted, skipping",
                       cf_name_.c_str(), file_num_buf, loop);

      sr = nullptr;
    }

    // This file is not being compacted. Consider it as the
    // first candidate to be compacted.
    uint64_t candidate_size = sr != nullptr ? sr->compensated_file_size : 0;
    if (sr != nullptr) {
      char file_num_buf[kFormatFileNumberBufSize];
      sr->Dump(file_num_buf, sizeof(file_num_buf), true);
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] Universal: Possible candidate %s[%d].",
                       cf_name_.c_str(), file_num_buf, loop);
    }

    // Check if the succeeding files need compaction.
    for (size_t i = loop + 1;
         candidate_count < max_files_to_compact && i < sorted_runs_.size();
         i++) {
      const IndexTree* succeeding_sr = &sorted_runs_[i];
      if (succeeding_sr->being_compacted) {
        break;
      }
      // Pick files if the total/last candidate file size (increased by the
      // specified ratio) is still larger than the next candidate file.
      // candidate_size is the total size of files picked so far with the
      // default kCompactionStopStyleTotalSize; with
      // kCompactionStopStyleSimilarSize, it's simply the size of the last
      // picked file.
      double sz = candidate_size * (100.0 + ratio) / 100.0;
      if (sz < static_cast<double>(succeeding_sr->size)) {
        break;
      }
      if (mutable_cf_options_.compaction_options_universal.stop_style ==
          kCompactionStopStyleSimilarSize) {
        // Similar-size stopping rule: also check the last picked file isn't
        // far larger than the next candidate file.
        sz = (succeeding_sr->size * (100.0 + ratio)) / 100.0;
        if (sz < static_cast<double>(candidate_size)) {
          // If the small file we've encountered begins a run of similar-size
          // files, we'll pick them up on a future iteration of the outer
          // loop. If it's some lonely straggler, it'll eventually get picked
          // by the last-resort read amp strategy which disregards size ratios.
          break;
        }
        candidate_size = succeeding_sr->compensated_file_size;
      } else {  // default kCompactionStopStyleTotalSize
        candidate_size += succeeding_sr->compensated_file_size;
      }
      candidate_count++;
    }

    // Found a series of consecutive files that need compaction.
    if (candidate_count >= (unsigned int)min_merge_width) {
      start_index = loop;
      done = true;
      break;
    } else {
      for (size_t i = loop;
           i < loop + candidate_count && i < sorted_runs_.size(); i++) {
        const IndexTree* skipping_sr = &sorted_runs_[i];
        char file_num_buf[256];
        skipping_sr->DumpSizeInfo(file_num_buf, sizeof(file_num_buf), loop);
        ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: Skipping %s",
                         cf_name_.c_str(), file_num_buf);
      }
    }
  }
  if (!done || candidate_count <= 1) {
    return nullptr;
  }
  size_t first_index_after = start_index + candidate_count;
  // Compression is enabled if files compacted earlier already reached
  // size ratio of compression.
  bool enable_compression = true;
  int ratio_to_compress =
      mutable_cf_options_.compaction_options_universal.compression_size_percent;
  if (ratio_to_compress >= 0) {
    uint64_t total_size = 0;
    for (auto& sorted_run : sorted_runs_) {
      total_size += sorted_run.compensated_file_size;
    }

    uint64_t older_file_size = 0;
    for (size_t i = sorted_runs_.size() - 1; i >= first_index_after; i--) {
      older_file_size += sorted_runs_[i].size;
      if (older_file_size * 100L >= total_size * (long)ratio_to_compress) {
        enable_compression = false;
        break;
      }
    }
  }

  uint64_t estimated_total_size = 0;
  for (unsigned int i = 0; i < first_index_after; i++) {
    estimated_total_size += sorted_runs_[i].size;
  }
  int start_level = sorted_runs_[start_index].level;
  int output_level;
  if (first_index_after == sorted_runs_.size()) {
    output_level = vstorage_->num_levels() - 1;
  } else if (sorted_runs_[first_index_after].level == 0) {
    output_level = 0;
  } else {
    output_level = sorted_runs_[first_index_after].level - 1;
  }

  uint32_t path_id = GetPathId(ioptions_, mutable_cf_options_, output_level);
  // last level is reserved for the files ingested behind
  if (ioptions_.allow_ingest_behind &&
      (output_level == vstorage_->num_levels() - 1)) {
    assert(output_level > 1);
    output_level--;
  }

  std::vector<CompactionInputFiles> inputs(vstorage_->num_levels());
  for (size_t i = 0; i < inputs.size(); ++i) {
    inputs[i].level = start_level + static_cast<int>(i);
  }
  for (size_t i = start_index; i < first_index_after; i++) {
    auto& picking_sr = sorted_runs_[i];
    if (picking_sr.level == 0) {
      FileMetaData* picking_file = picking_sr.file;
      inputs[0].files.push_back(picking_file);
    } else {
      auto& files = inputs[picking_sr.level - start_level].files;
      for (auto* f : vstorage_->LevelFiles(picking_sr.level)) {
        files.push_back(f);
      }
    }
    char file_num_buf[256];
    picking_sr.DumpSizeInfo(file_num_buf, sizeof(file_num_buf), i);
    ROCKS_LOG_BUFFER(log_buffer_, "[%s] Universal: Picking %s",
                     cf_name_.c_str(), file_num_buf);
  }

  CompactionReason compaction_reason;
  if (max_number_of_files_to_compact == UINT_MAX) {
    compaction_reason = CompactionReason::kUniversalSizeRatio;
  } else {
    compaction_reason = CompactionReason::kUniversalSortedRunNum;
  }
  return new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, std::move(inputs),
      output_level,
      MaxFileSizeForLevel(mutable_cf_options_, output_level,
                          kCompactionStyleUniversal),
      LLONG_MAX, path_id,
      GetCompressionType(ioptions_, vstorage_, mutable_cf_options_, start_level,
                         1, enable_compression),
      GetCompressionOptions(mutable_cf_options_, vstorage_, start_level,
                            enable_compression),
      /* max_subcompactions */ 0, /* grandparents */ {}, /* is manual */ false,
      score_, false /* deletion_compaction */, compaction_reason);
}

Compaction* GearCompactionBuilder::PickCompactionLastLevel() {
  ROCKS_LOG_BUFFER(log_buffer_, "[%s] Gear: Last Level Compaction",
                   cf_name_.c_str());
  int last_level = vstorage_->num_levels() - 1;
  auto l2_trees = tree_level_map[last_level].second;
  if (l2_trees[VersionStorageInfo::l2_small_tree_index].being_compacted ||
      l2_trees[VersionStorageInfo::l2_large_tree_index].being_compacted) {
    // any of the L2 files should not being compacted.
    return nullptr;
  }

  Compaction* c = PickCompactionToOldest(
      sorted_runs_.size() - 2, CompactionReason::kGearCompactionAllInOne);

  TEST_SYNC_POINT_CALLBACK(
      "UniversalCompactionPicker::PickCompactionLastLevel:Return", c);

  return c;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE
