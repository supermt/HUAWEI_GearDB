// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE
#include <stdint.h>

#include <memory>
#include <string>

#include "options/options_helper.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {

struct EnvOptions;

class Status;
class RandomAccessFile;
class WritableFile;
class Table;
class TableBuilder;

class GearTableFactory : public TableFactory {
 public:
  ~GearTableFactory() {}
  explicit GearTableFactory(
      const GearTableOptions& _table_options = GearTableOptions())
      : table_options_(_table_options) {}

  const char* Name() const override { return "GearTable"; }
  Status NewTableReader(
      const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache = true) const override;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, WritableFileWriter* file) const override;

  std::string GetPrintableTableOptions() const override;

  const GearTableOptions& table_options() const;

  static const char kValueTypeSeqId0 = char(~0);

  // Sanitizes the specified DB Options.
  Status SanitizeOptions(
      const DBOptions& /*db_opts*/,
      const ColumnFamilyOptions& /*cf_opts*/) const override {
    return Status::OK();
  }

  void* GetOptions() override { return &table_options_; }

  Status GetOptionString(const ConfigOptions& /*config_options*/,
                         std::string* /*opt_string*/) const override {
    return Status::OK();
  }

 private:
  GearTableOptions table_options_;
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
