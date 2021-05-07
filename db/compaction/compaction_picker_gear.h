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

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {
class GearCompactionPicker : public CompactionPicker {
 public:
  GearCompactionPicker(const ImmutableCFOptions& ioptions,
                       const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}
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

}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
