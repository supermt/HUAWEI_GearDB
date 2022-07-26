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
  if (earliest_memtable_seqno == kMaxSequenceNumber) {
    // No matter how, we need to trigger a deep compaction
    builder.force_compaction_ = true;
  }
  return builder.PickCompaction();
}

Compaction* GearCompactionBuilder::PickCompaction() {
  const int kLevel0 = 0;
  Compaction* c = nullptr;

  l0_score_ = vstorage_->CompactionScore(kLevel0);
  for (int i = vstorage_->num_levels() - 2; i >= 0; i--) {
    if (vstorage_->CompactionScore(i) > 1) {
      c = PickCompactionForLevel(i);
    }
  }

  if ((L2SmallTreeIsFilled() || force_compaction_) &&
      !picker_->IsAllInOneCompactionInProgress()) {
    ROCKS_LOG_BUFFER_MAX_SZ(
        log_buffer_, 100,
        "[%s] Gear Compaction: l2 compaction is not in progress\n");
    c = PickCompactionLastLevel();
  }

  // there's no delete compaction in Gear Compaction
  if (c == nullptr) {
    return nullptr;
  }

  // update statistics
  RecordInHistogram(ioptions_.statistics, NUM_FILES_IN_SINGLE_COMPACTION,
                    c->inputs(0)->size());

  picker_->RegisterCompaction(c);
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);

  return c;
}

Compaction* GearCompactionBuilder::PickCompactionForLevel(int level) {
  auto max_number_of_files = 64ul;
  std::vector<CompactionInputFiles> inputs(1);
  CompactionInputFiles* input;
  input = &(inputs[0]);

  input->level = level;
  uint64_t number_of_candidates = 0;
  uint64_t bytes_of_candidates = 0;

  std::stringstream candidate_file_id;

  candidate_file_id << "[";

  for (auto f : vstorage_->LevelFiles(level)) {
    // add all files not in compaction states
    if (!f->being_compacted) {
      input->files.push_back(f);
      number_of_candidates++;
      bytes_of_candidates += (f->estimate_num_blocks * 8192);
      candidate_file_id << f->fd.GetNumber() << " ";
    }
    if (number_of_candidates > max_number_of_files ||
        bytes_of_candidates > max_compaction_file_size) {
      break;
    }
  }

  candidate_file_id << "]";
  ROCKS_LOG_BUFFER(log_buffer_,
                   "[%s] Gear : %s collection from level %d, file ids: %s",
                   cf_name_.c_str(), "level-collection compactions", level,
                   candidate_file_id.str().c_str());
  int start_level = level;
  int output_level = level + 1;

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
      {}, false, l0_score_, false, CompactionReason::kGearCollectTiered);
}

Compaction* GearCompactionBuilder::PickCompactionLastLevel() {
  ROCKS_LOG_BUFFER(log_buffer_, "[%s] Gear: Last Level Compaction",
                   cf_name_.c_str());
  int last_level = vstorage_->num_levels() - 1;

  std::vector<CompactionInputFiles> inputs(1);
  CompactionInputFiles* input_small = &inputs[0];
  //  CompactionInputFiles* input_large = &inputs[1];
  input_small->level = last_level;  // there's only one level;
                                    //  input_large->level = last_level;

  for (auto&& l2_sst : vstorage_->LevelFiles(last_level)) {
    if (l2_sst->l2_position == VersionStorageInfo::l2_small_tree_index)
      input_small->files.push_back(l2_sst);
  }
  InternalKey smallest, largest;
  picker_->GetRange(inputs, &smallest, &largest);
  vstorage_->GetOverlappingInputs(last_level, &smallest, &largest,
                                  &(input_small->files));

  // collect as many overlapped l2 ssts as possible

  uint32_t path_id = GetPathId(ioptions_, mutable_cf_options_, last_level);
  Compaction* c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, std::move(inputs),
      vstorage_->num_levels() - 1,
      MaxFileSizeForLevel(mutable_cf_options_, last_level,
                          kCompactionStyleGear),
      mutable_cf_options_.max_compaction_bytes, path_id,
      GetCompressionType(ioptions_, vstorage_, mutable_cf_options_, last_level,
                         vstorage_->base_level(), true),
      GetCompressionOptions(mutable_cf_options_, vstorage_, last_level), 0, {},
      false, l0_score_, false, CompactionReason::kGearCompactionAllInOne);
  return c;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !ROCKSDB_LITE
