//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <env/io_posix.h>
#include <rocksdb/file_system.h>
#include <rocksdb/io_status.h>

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "memory/arena.h"
#include "monitoring/histogram.h"
#include "options/cf_options.h"
#include "rocksdb/options.h"
#include "table/gear_block/btree_index/persistent_btree.h"

namespace ROCKSDB_NAMESPACE {

class GearTableIndexReader {
 public:
  enum IndexSearchResult { kNotFound = 0, kDirectToFile = 1 };

  static std::string find_the_index_by_file_name(
      const ImmutableCFOptions& ioptions,
      std::basic_string<char> ori_file_name) {
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

  explicit GearTableIndexReader(std::string index_file_name) {
    btree_open(btree, index_file_name.c_str());
  }

  IndexSearchResult GetOffset(Slice key, uint32_t* target_offset) const;

  uint32_t GetIndexSize() const { return index_size_; }

  static const uint64_t kMaxFileSize = (1u << 31) - 1;
  static const size_t kOffsetLen = sizeof(uint32_t);

 private:
  uint32_t index_size_;
  BTree* btree;
};

// the gear table index builder will create a Btree and save it into the file.
class GearTableIndexBuilder {
 public:
  GearTableIndexBuilder(Arena* arena, const ImmutableCFOptions& ioptions,
                        std::string& ori_filename)
      : arena_(arena), ioptions_(ioptions) {
    std::string index_file_name =
        GearTableIndexReader::find_the_index_by_file_name(ioptions_,
                                                          ori_filename);
    int open_result = btree_open(btree, index_file_name.c_str());
    assert(open_result != -1);
  }

  void AddKeyOffset(Slice key, uint32_t key_offset);

  IOStatus Finish();

  uint32_t GetTotalSize() const { return 0; }

  static const std::string kGearTableIndexBlock;

 private:
  BTree* btree;

  Arena* arena_;
  const ImmutableCFOptions ioptions_;
  WritableFileWriter* file_writer_;
  const SliceTransform* prefix_extractor_;
};

};  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
