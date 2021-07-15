//
// Created by jinghuan on 7/6/21.
//
#include <iostream>

#include "rocksdb/gear_bench.h"
#include "rocksdb/gear_bench_classes.h"
#ifdef GFLAGS
#include <gflags/gflags.h>
#ifdef NUMA
#include <numa.h>
#include <numaif.h>
#endif
#ifndef OS_WIN
#include <unistd.h>
#endif
#include <fcntl.h>
#include <rocksdb/sst_file_reader.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <condition_variable>
#include <cstddef>
#include <iterator>
#include <memory>
#include <mutex>
#include <queue>
#include <regex>
#include <thread>
#include <unordered_map>

#include "db/db_impl/db_impl.h"
#include "db/malloc_stats.h"
#include "db/version_set.h"
#include "hdfs/env_hdfs.h"
#include "monitoring/histogram.h"
#include "monitoring/statistics.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/stats_history.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/sim_cache.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/write_batch.h"
#include "test_util/testutil.h"
#include "test_util/transaction_test_util.h"
#include "util/cast_util.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/gflags_compat.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"
#include "util/xxhash.h"
#include "utilities/DOTA/report_agent.h"
#include "utilities/blob_db/blob_db.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/bytesxor.h"
#include "utilities/merge_operators/sortlist.h"
#include "utilities/persistent_cache/block_cache_tier.h"

#ifdef MEMKIND
#include "memory/memkind_kmem_allocator.h"
#endif

#ifdef OS_WIN
#include <io.h>  // open/close
#endif
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_uint64(seed, 0, "random seed");
DEFINE_string(benchmark, "",
              "available values: "
              "[\'generate,merge\'] for generate and merge,\n"
              " \'generate\' for creat l2 big tree only \n"
              "\'merge\' for generate a l2 small tree, and trigger a L2 All In "
              "One Merge \n"
              "Please ensure when there is a merge operation in the benchmark, "
              "the use_existing_data is triggered");
// directory settings.
DEFINE_bool(use_existing_data, true, "Use the existing database or not");
DEFINE_bool(delete_new_files, true, "Delete L2 small tree after bench");
DEFINE_string(db, "", "The database path");
// key range settings.
DEFINE_double(span_range, 0.1, "The overlapping range of ");
DEFINE_double(min_value, 0, "The min values of the key range");
DEFINE_uint64(distinct_num, 80000000000, "number of distinct entries");
DEFINE_uint64(existing_entries, 80000000000,
              "The number of entries inside existing database, this option "
              "will be ignored while use_existing_data is triggered");
DEFINE_uint64(l2_small_size, 500000 * 64,
              "Size (entry count) of L2 Small tree");

// Key size settings.
DEFINE_int32(key_size, 15, "size of each key");
DEFINE_int32(value_size, 10, "size of each value");

// DB column settings
DEFINE_int32(num_column_families, 1, "Number of Column Families to use.");
DEFINE_int32(threads, 1, "Number of concurrent threads to run.");
DEFINE_uint64(write_buffer_size, 500000,
              "Size of Memtable, each flush will directly create a l2 small "
              "tree spanning in the entire key space");
static ROCKSDB_NAMESPACE::Env* FLAGS_env = ROCKSDB_NAMESPACE::Env::Default();
DEFINE_int64(report_interval_seconds, 0,
             "If greater than zero, it will write simple stats in CVS format "
             "to --report_file every N seconds");

DEFINE_string(report_file, "report.csv",
              "Filename where some simple stats are reported to (if "
              "--report_interval_seconds is bigger than 0)");
DEFINE_string(index_dir_prefix, "index", "the index directory");
DEFINE_bool(print_data, false, "print out the keys with in HEX mode");

namespace ROCKSDB_NAMESPACE {

void constant_options(Options& opt) {
  opt.compaction_style = kCompactionStyleGear;
  opt.num_levels = 3;
  // specific type of sstable format.
  GearTableOptions gearTableOptions;
  gearTableOptions.encoding_type = kPlain;
  gearTableOptions.user_key_len = FLAGS_key_size;
  gearTableOptions.user_value_len = FLAGS_value_size;
  opt.table_factory =
      std::shared_ptr<TableFactory>(NewGearTableFactory(gearTableOptions));

  opt.ttl = 0;
  opt.periodic_compaction_seconds = 0;
}

void ConfigByGFLAGS(Options& opt) {
  opt.create_if_missing = !FLAGS_use_existing_data;
  opt.env = FLAGS_env;
}

uint64_t max_key_num() {
  // TODO: fill the function by reading through all the SSTables in L2
  return std::numeric_limits<uint64_t>::max();
}

void StartBenchmark(Options& start_options) {
  //  BenchMark bench;
  //  // TODO: maybe we need to add the Open() function for gear compaction
  //  std::cout << "start gear_table_tool" << std::endl;
  //  bench.GenerateDB(start_options);
}

void GenerateKeyRange() {
  std::cout << "calculating the number of distinct number" << std::endl;
  uint64_t max_key_in_file = max_key_num();
  uint64_t distinct_num = std::max(FLAGS_distinct_num, max_key_in_file);
  std::cout << "Distinct num in options is: " << FLAGS_distinct_num
            << std::endl;
  std::cout << "Max key in existing tables is " << max_key_in_file << std::endl;
  std::cout << "we choose the max of them, which is " << distinct_num
            << std::endl;
}

Options BootStrap(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_db.back() != '/') {
    FLAGS_db.push_back('/');
  }
  std::cout << "db at " << FLAGS_db << std::endl;
  Options basic_options;
  basic_options.create_if_missing = !FLAGS_use_existing_data;
  basic_options.db_paths.emplace_back(FLAGS_db,
                                      std::numeric_limits<uint64_t>::max());
  basic_options.index_dir_prefix = FLAGS_index_dir_prefix;
  return basic_options;
}

int gear_bench(int argc, char** argv) {
  //
  Options basic_options = BootStrap(argc, argv);
  constant_options(basic_options);
  L2SmallTreeCreator l2_small_gen =
      L2SmallTreeCreator(FLAGS_db + "l2_small.sst", basic_options, FLAGS_env,
                         FLAGS_print_data, FLAGS_delete_new_files);
  // Prepare the random generators
  Random64 rand_gen(FLAGS_seed);
  KeyGenerator key_gen(&rand_gen, SEQUENTIAL, FLAGS_distinct_num, FLAGS_seed,
                       FLAGS_key_size, FLAGS_min_value);

  // Create the picker dummy, use this to create the compacted levels.
  CompactionPickerDummy pickerDummy(basic_options);
  pickerDummy.NewVersionStorage(3, kCompactionStyleGear);
  pickerDummy.UpdateVersionStorageInfo();

  // Create the mock file generator
  MockFileGenerator l2_big_tree_gen(FLAGS_env, FLAGS_db, basic_options);
  l2_big_tree_gen.NewDB(FLAGS_use_existing_data);

  // Preparation finished
  std::stringstream benchmark_stream(FLAGS_benchmark);
  std::string name;
  while (std::getline(benchmark_stream, name, ',')) {
    if (name == "merge") {
      std::vector<std::string> keys;
      std::string temp;
      for (uint64_t i = 0; i < 10; i++) {
        temp = key_gen.NextString();
        //    std::cout << key.ToString(true) << std::endl;
        keys.push_back(temp);
      }
      l2_small_gen.CreateFileAndCheck(keys);

    } else if (name == "generate") {
      int l2_big_tree_num = FLAGS_distinct_num / FLAGS_write_buffer_size;
      std::cout << l2_big_tree_num << " SSTs need creatation" << std::endl;
      assert(FLAGS_use_existing_data == false);
      for (int file_num = 0; file_num < l2_big_tree_num; file_num++) {
        uint64_t smallest_key =
            file_num * FLAGS_write_buffer_size + FLAGS_min_value;
        uint64_t largest_key = smallest_key + FLAGS_write_buffer_size;
        largest_key = std::min(largest_key, FLAGS_distinct_num);
        std::string smallest_key_str;
        std::string largest_key_str;
        smallest_key_str = key_gen.GenerateKeyFromInt(smallest_key);
        largest_key_str = key_gen.GenerateKeyFromInt(largest_key);

        l2_big_tree_gen.CreateFileByKeyRange(smallest_key, largest_key,
                                             &key_gen);
        std::cout << "No. " << file_num
                  << " SST generated, smallest key: " << smallest_key_str
                  << " largest key: " << largest_key_str << std::endl;
      }
      if (l2_big_tree_num * FLAGS_write_buffer_size < FLAGS_distinct_num) {
        uint64_t smallest_key =
            l2_big_tree_num * FLAGS_write_buffer_size + FLAGS_min_value;
        uint64_t largest_key = smallest_key + FLAGS_write_buffer_size;
        largest_key = std::min(largest_key, FLAGS_distinct_num);
        std::string smallest_key_str;
        std::string largest_key_str;
        smallest_key_str = key_gen.GenerateKeyFromInt(smallest_key);
        largest_key_str = key_gen.GenerateKeyFromInt(largest_key);

        l2_big_tree_gen.CreateFileByKeyRange(smallest_key, largest_key,
                                             &key_gen);
        std::cout << "No. " << l2_big_tree_num
                  << " SST generated, smallest key: " << smallest_key_str
                  << " largest key: " << largest_key_str << std::endl;
      }
      std::cout << "l2 big tree generated" << std::endl;

    } else {
      return -1;
    }
  }
  l2_big_tree_gen.FreeDB();

  return 0;
}

}  // namespace ROCKSDB_NAMESPACE
#endif