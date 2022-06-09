//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE
#include <cstdint>
#include <string>
#include <vector>

#include "db/version_edit.h"
#include "gear_table_index.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "table/gear_block/gear_table_file_reader.h"
#include "table/gear_block/gear_table_index.h"
#include "table/plain/plain_table_bloom.h"
#include "table/plain/plain_table_index.h"
#include "table/plain/plain_table_key_coding.h"
#include "table/table_builder.h"

namespace ROCKSDB_NAMESPACE {

class BlockBuilder;
class BlockHandle;
class WritableFile;
class TableBuilder;

// The builder class of GearTable. For description of GearTable format
// See comments of class GearTableFactory, where instances of
// GearTableReader are created.
class GearTableBuilder : public TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish(). The output file
  // will be part of level specified by 'level'.  A value of -1 means
  // that the caller does not know which level the output file will reside.
  GearTableBuilder(
      const ImmutableCFOptions& ioptions, const MutableCFOptions& moptions,
      const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
          int_tbl_prop_collector_factories,
      uint32_t column_family_id, WritableFileWriter* file,
      uint32_t user_key_len, uint32_t user_value_len,
      EncodingType encoding_type, const std::string& column_family_name,
      int target_level);
  //      , WritableFileWriter* index_file);
  // No copying allowed
  GearTableBuilder(const GearTableBuilder&) = delete;
  void operator=(const GearTableBuilder&) = delete;

  static uint32_t CalculateHeaderSize() { return kGearTableHeaderLength; }
  // REQUIRES: Either Finish() or Abandon() has been called.
  ~GearTableBuilder() override;

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  void AddPack(std::string data_packs);
  // Return non-ok iff some error has been detected.
  Status status() const override { return status_; }

  // Return non-ok iff some error happens during IO.
  IOStatus io_status() const override { return io_status_; }

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

  TableProperties GetTableProperties() const override { return properties_; }

  // Get file checksum
  std::string GetFileChecksum() const override;

  // Get file checksum function name
  const char* GetFileChecksumFuncName() const override;

 private:
  Arena arena_;
  const ImmutableCFOptions& ioptions_;
  const MutableCFOptions& moptions_;
  std::vector<std::unique_ptr<IntTblPropCollector>>
      table_properties_collectors_;

  std::unique_ptr<GearTableIndexBuilder> index_builder_;

  WritableFileWriter* file_;
  //  WritableFileWriter* index_file_;
  uint64_t offset_ = 0;
  uint32_t current_key_length;
  uint32_t current_value_length;
  Status status_;
  IOStatus io_status_;
  TableProperties properties_;
  std::string block_header_buffer;
  std::string block_value_buffer;
  std::string block_key_buffer;
  uint32_t page_entry_count;

  std::vector<uint32_t> keys_or_prefixes_hashes_;

  bool closed_ = false;  // Either Finish() or Abandon() has been called.

  const SliceTransform* prefix_extractor_;
  uint64_t estimate_size_limit_;
  uint64_t data_block_size = GearTableFileReader::PAGE_SIZE;
  Slice GetPrefix(const Slice& target) const {
    assert(target.size() >= 8);  // target is internal key
    return GetPrefixFromUserKey(GetUserKey(target));
  }

  Slice GetPrefix(const ParsedInternalKey& target) const {
    return GetPrefixFromUserKey(target.user_key);
  }
  void FlushDataBlock();

  Slice GetUserKey(const Slice& key) const {
    return Slice(key.data(), key.size() - 8);
  }

  Slice GetPrefixFromUserKey(const Slice& user_key) const {
    if (!IsTotalOrderMode()) {
      return prefix_extractor_->Transform(user_key);
    } else {
      // Use empty slice as prefix if prefix_extractor is not set.
      // In that case,
      // it falls back to pure binary search and
      // total iterator seek is supported.
      return Slice();
    }
  }

  bool IsTotalOrderMode() const { return (prefix_extractor_ == nullptr); }
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE