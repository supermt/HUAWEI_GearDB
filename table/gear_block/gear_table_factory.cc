//
// Created by jinghuan on 6/12/21.
//

#ifndef ROCKSDB_LITE
#include "table/gear_block/gear_table_factory.h"

#include <stdint.h>

#include <memory>

#include "db/dbformat.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "table/gear_block/gear_table_builder.h"
#include "table/gear_block/gear_table_reader.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
static std::unordered_map<std::string, OptionTypeInfo> plain_table_type_info = {
    {"user_key_len",
     {offsetof(struct GearTableOptions, user_key_len), OptionType::kUInt32T,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"user_value_len",
     {offsetof(struct GearTableOptions, user_key_len), OptionType::kUInt32T,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"encoding_type",
     {offsetof(struct GearTableOptions, encoding_type),
      OptionType::kEncodingType, OptionVerificationType::kByName,
      OptionTypeFlags::kNone, 0}},
};

Status GearTableFactory::NewTableReader(
    const TableReaderOptions &table_reader_options,
    std::unique_ptr<RandomAccessFileReader> &&file, uint64_t file_size,
    std::unique_ptr<TableReader> *table,
    bool /*prefetch_index_and_filter_in_cache*/) const {
  // Create the external index file when the file.

  return GearTableReader::Open(table_reader_options.ioptions,
                               table_reader_options.env_options,
                               table_reader_options.internal_comparator,
                               std::move(file), file_size, table);
  //  return Status::OK();
  return GearTableReader::Open(
      table_reader_options.ioptions, table_reader_options.env_options,
      table_reader_options.internal_comparator, std::move(file), file_size,
      table, table_reader_options.immortal,
      table_reader_options.prefix_extractor);
}

TableBuilder *GearTableFactory::NewTableBuilder(
    const TableBuilderOptions &table_builder_options, uint32_t column_family_id,
    rocksdb::WritableFileWriter *file) const {
  return nullptr;
}

std::string GearTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  user_key_len: %u\n",
           table_options_.user_key_len);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  user_value_len: %u\n",
           table_options_.user_value_len);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  encoding_type: %d\n",
           table_options_.encoding_type);
  ret.append(buffer);
  return ret;
}

const GearTableOptions &GearTableFactory::table_options() const {
  return table_options_;
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE