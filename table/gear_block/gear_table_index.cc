//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "table/gear_block/gear_table_index.h"

#include <cinttypes>

#include "table/gear_block/gear_table_builder.h"
#include "util/coding.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

const std::string GearTableIndexBuilder::kGearTableIndexBlock =
    "GearTableIndexBlock";
void GearTableIndexBuilder::AddKeyOffset(Slice key, uint32_t key_offset) {
  std::pair<std::string, uint32_t> index_pair;
  btree_insert(btree, reinterpret_cast<const uint8_t *>(key.data()),
               &key_offset, sizeof(key_offset));
}

IOStatus GearTableIndexBuilder::Finish() { return this->file_writer_->Flush(); }

GearTableIndexReader::IndexSearchResult GearTableIndexReader::GetOffset(
    rocksdb::Slice key, uint32_t *target_offset) const {
  const uint8_t *search_key = reinterpret_cast<const uint8_t *>(key.data());
  size_t value_size = kOffsetLen;
  //      sizeof(uint32_t);
  uint32_t *result =
      static_cast<uint32_t *>(btree_get(btree, search_key, &value_size));
  if (result == nullptr) {
    *target_offset = *result;
    return kDirectToFile;
  } else {
    return kNotFound;
  }
}

};  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
