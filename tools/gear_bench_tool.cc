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
//--db=/media/jinghuan/nvme/gear_bench_test
//    --distinct_num=1000000
//        --write_buffer_size=500000
//            --benchmark=generate,merge
//            --use_existing_data=False
//                --span_range=1
//                    --l2_small_tree_num=2
//                        --max_background_compactions=4
//                            --target_file_size_base=500000
DEFINE_uint64(seed, 0, "random seed");
DEFINE_bool(statistics, false,
            "print out the detailed statistics after execution");
DEFINE_string(benchmark, "validate,generate,merge",
              "available values: "
              "[\'generate,merge\'] for generate and merge,\n"
              " \'generate\' for creat l2 big tree only \n"
              "\'merge\' for generate a l2 small tree, and trigger a L2 All In "
              "One Merge \n"
              "Please ensure when there is a merge operation in the benchmark, "
              "the use_existing_data is triggered");
// directory settings.
DEFINE_bool(use_exist_db, false, "Use the existing database or not");
DEFINE_bool(reload_keys, false, "Use the existing database or not");
DEFINE_bool(delete_new_files, true, "Delete L2 small tree after bench");
DEFINE_string(db_path, "/tmp/rocksdb/gear", "The database path");
DEFINE_string(table_format, "gear", "available formats: gear or normal");
DEFINE_int64(max_open_files, 100, "max_opened_files");
DEFINE_int64(bench_threads, 1, "number of working threads");
DEFINE_int64(write_batch_size, 10000, "number of working threads");

// key range settings.
DEFINE_int64(duration, 0, "Duration of Fill workloads");
DEFINE_double(span_range, 1.0, "The overlapping range of ");
DEFINE_double(min_value, 0, "The min values of the key range");
DEFINE_uint64(distinct_num, 1000000, "number of distinct entries");
DEFINE_uint64(existing_entries, 8000000000,
              "The number of entries inside existing database, this option "
              "will be ignored while use_existing_data is triggered");
DEFINE_uint64(l2_small_tree_num, 2, "Num of SST files in L2 Small tree");

// Key size settings.
DEFINE_int32(key_size, 8, "size of each user key");
DEFINE_int32(value_size, 10, "size of each value");
// DB column settings
DEFINE_int32(max_background_compactions, 1,
             "Number of concurrent threads to run.");
DEFINE_uint64(write_buffer_size, 312 * 800,
              "Size of Memtable, each flush will directly create a l2 small "
              "tree spanning in the entire key space");
DEFINE_uint64(target_file_size_base, 312 * 800,
              "Size of Memtable, each flush will directly create a l2 small "
              "tree spanning in the entire key space");
DEFINE_uint64(max_compaction_bytes, 50000000000,
              "max compaction, too small value will cut the SSTables into very "
              "small pieces.");
static ROCKSDB_NAMESPACE::Env* FLAGS_env = ROCKSDB_NAMESPACE::Env::Default();
// DEFINE_int64(report_interval_seconds, 0,
//               "If greater than zero, it will write simple stats in CVS format
//               " "to --report_file every N seconds");
//
//  DEFINE_string(report_file, "report.csv",
//                "Filename where some simple stats are reported to (if "
//           c     "--report_interval_seconds is bigger than 0)");
DEFINE_string(index_dir_prefix, "index", "the index directory");
DEFINE_bool(print_data, false, "print out the keys with in HEX mode");

DEFINE_string(injection_sst_dir, "sst_files", "place to load the SST files");

namespace ROCKSDB_NAMESPACE {
void constant_options(Options& opt) {
  opt.max_open_files = 128;
  opt.compaction_style = kCompactionStyleGear;
  opt.num_levels = 3;
  opt.ttl = 0;
  opt.periodic_compaction_seconds = 0;
  opt.report_bg_io_stats = true;
}

void ConfigByGFLAGS(Options& opt) {
  opt.create_if_missing = !FLAGS_use_exist_db;
  opt.max_open_files = FLAGS_max_open_files;
  opt.env = FLAGS_env;
  opt.write_buffer_size =
      FLAGS_write_buffer_size * (FLAGS_key_size + FLAGS_value_size);
  opt.target_file_size_base =
      FLAGS_target_file_size_base * (FLAGS_key_size + FLAGS_value_size);
  opt.max_background_jobs = FLAGS_max_background_compactions + 1;
  opt.index_dir_prefix = FLAGS_index_dir_prefix;
  opt.max_subcompactions = FLAGS_max_background_compactions;
  opt.max_compaction_bytes =
      std::max(FLAGS_max_compaction_bytes, opt.target_file_size_base);

  if (FLAGS_table_format == "gear") {
    // specific type of sstable format.
    GearTableOptions gearTableOptions;
    gearTableOptions.encoding_type = kPlain;
    gearTableOptions.user_key_len = FLAGS_key_size;
    gearTableOptions.user_value_len = FLAGS_value_size;
    opt.table_factory =
        std::shared_ptr<TableFactory>(NewGearTableFactory(gearTableOptions));
  } else {
    opt.table_factory =
        std::shared_ptr<TableFactory>(NewBlockBasedTableFactory());
  }
}

Options BootStrap(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_db_path.back() != '/') {
    FLAGS_db_path.push_back('/');
  }
  std::cout << "db at " << FLAGS_db_path << std::endl;
  Options basic_options;
  basic_options.create_if_missing = !FLAGS_use_exist_db;
  basic_options.db_paths.emplace_back(FLAGS_db_path,
                                      std::numeric_limits<uint64_t>::max());
  basic_options.index_dir_prefix = FLAGS_index_dir_prefix;
  return basic_options;
}

void DoMerge(MockFileGenerator& mock_db, KeyGenerator* key_gen) {
  FLAGS_use_exist_db = true;
  std::cout << "Start the merging" << std::endl;
  mock_db.ReOpenDB();
  // Validate the version.
  uint64_t total_key_range = FLAGS_distinct_num - FLAGS_min_value;
  uint64_t merge_key_range = total_key_range * FLAGS_span_range;
  uint64_t single_file_range = merge_key_range / FLAGS_l2_small_tree_num;
  if (merge_key_range < FLAGS_write_buffer_size ||
      single_file_range < FLAGS_write_buffer_size) {
    std::cout << "can't fill one single SST, try to decrease the SSTable "
                 "size or increase the overlapping range."
              << std::endl;
    return;
  }
  SequenceNumber seq = mock_db.versions_->LastSequence();
  auto files = mock_db.cfd_->current()->storage_info()->LevelFiles(2);
  bool small_tree_generated = false;
  for (auto f : files) {
    if (f->l2_position == VersionStorageInfo::l2_small_tree_index) {
      small_tree_generated = true;
    }
  }
  auto start_time = FLAGS_env->NowMicros();
  if (!small_tree_generated) {
    std::cout << "Start generating new l2 small tree at" << start_time
              << std::endl;
    for (uint64_t i = 0; i < FLAGS_l2_small_tree_num; i++) {
      stl_wrappers::KVMap content;
      uint64_t smallest = FLAGS_min_value + i * single_file_range;
      uint64_t largest = FLAGS_min_value + (i + 1) * single_file_range - 1;
      std::cout << "New generated file range:" << smallest << "," << largest
                << std::endl;
      largest = std::min(largest, FLAGS_distinct_num);
      SpanningKeyGenerator l2_small_key_gen(
          smallest, largest,
          std::min(FLAGS_write_buffer_size, largest - smallest), FLAGS_seed,
          SpanningKeyGenerator::kUniform);
      content = l2_small_key_gen.GenerateContent(key_gen, &seq);
      Status s = mock_db.AddMockFile(
          content, 2, VersionStorageInfo::l2_small_tree_index, 0);
      if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
        abort();
      }
    }
    std::cout << "L2 Small tree generated: " << FLAGS_env->NowMicros()
              << std::endl;
  }

  //      mock_db.PrintFullTree(cfd);

  mock_db.TriggerCompaction();
  //      mock_db.PrintFullTree(cfd);
  //      mock_db.FreeDB();
}

int gear_bench(int argc, char** argv) {
  Options basic_options = BootStrap(argc, argv);
  constant_options(basic_options);
  ConfigByGFLAGS(basic_options);
  FLAGS_env->SetBackgroundThreads(2, ROCKSDB_NAMESPACE::Env::Priority::HIGH);
  FLAGS_env->SetBackgroundThreads(2, ROCKSDB_NAMESPACE::Env::Priority::BOTTOM);
  FLAGS_env->SetBackgroundThreads(2, ROCKSDB_NAMESPACE::Env::Priority::LOW);
  FLAGS_env->SetBackgroundThreads(
      1,  // there is only one for L2 Compaction
      ROCKSDB_NAMESPACE::Env::Priority::DEEP_COMPACT);

  FLAGS_env->SetBackgroundThreads(1,  // there is only one for L2 Compaction
                                  ROCKSDB_NAMESPACE::Env::Priority::BOTTOM);

  gear_db::Benchmark benchmark(FLAGS_use_exist_db, FLAGS_db_path,
                               basic_options);
  benchmark.Run();

  return 0;
}

// TODO: Add the write stall REASON
//

}  // namespace ROCKSDB_NAMESPACE

namespace gear_db {

void Benchmark::InitializeOptionsFromFlags(Options* opts) {
  ConfigByGFLAGS(*opts);
}

void Benchmark::Validate(ThreadState* thread) {
  std::cout << "Validating the files " << std::endl;
}
void Benchmark::Merge(ThreadState* thread) {
  std::cout << "Merging the files" << std::endl;
}
void Benchmark::Generate(ThreadState* thread) {
  SeqKeyGenerator key_gen(FLAGS_min_value);
  int start_file_num = 0;
  int l2_big_tree_num = FLAGS_distinct_num / FLAGS_write_buffer_size;
  int end_file_num = l2_big_tree_num;
  if (FLAGS_bench_threads > 1) {
    // we are using multi-threading methods to generate data.
    int file_num_each_thread = l2_big_tree_num / FLAGS_bench_threads + 1;
    std::cout << thread->tid << " " << file_num_each_thread << std::endl;
    start_file_num = thread->tid * file_num_each_thread;
    end_file_num = start_file_num + file_num_each_thread;
  }

  assert(FLAGS_use_exist_db == false);
  std::cout << "Creating " << (end_file_num - start_file_num) << " SSTs"
            << std::endl;

  auto start_ms = FLAGS_env->NowMicros();
  std::cout << "Start running at: " << start_ms << std::endl;
  for (int file_num = start_file_num; file_num < end_file_num; file_num++) {
    uint64_t smallest_key =
        file_num * FLAGS_write_buffer_size + FLAGS_min_value;
    uint64_t largest_key = smallest_key + FLAGS_write_buffer_size;
    largest_key = std::min(largest_key, FLAGS_distinct_num);
    mock_db_->CreateFileByKeyRange(smallest_key, largest_key, &key_gen,
                                   thread->tid);
    std::cout << "No. " << file_num << " SST generated at "
              << FLAGS_env->NowMicros() << ", smallest key: " << smallest_key
              << " largest key: " << largest_key << std::endl;
  }

  auto end_micro_sec = FLAGS_env->NowMicros();
  std::cout << "Time cost: " << ((double)end_micro_sec - start_ms) / 1000000
            << " sec.";
}

void Benchmark::Inject_LOAD(ThreadState* thread) {
  std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" << std::endl;
  std::cout << "Creating files for bulk loading" << std::endl;
  std::cout << "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
}

void Benchmark::Run() {
  std::stringstream benchmark_stream(FLAGS_benchmark);
  std::string name;

  if (FLAGS_reload_keys) {
    // Only work on single database
    assert(db_.db != nullptr);
    ReadOptions read_opts;
    read_opts.total_order_seek = true;
    Iterator* iter = db_.db->NewIterator(read_opts);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      keys_.emplace_back(iter->key().ToString());
    }
    delete iter;
    FLAGS_distinct_num = keys_.size();
  }

  while (std::getline(benchmark_stream, name, ',')) {
    // Sanitize parameters
    value_size_ = FLAGS_value_size;
    key_size_ = FLAGS_key_size;
    write_options_ = WriteOptions();
    write_options_.disableWAL = true;

    void (Benchmark::*method)(ThreadState*) = nullptr;
    void (Benchmark::*post_process_method)() = nullptr;

    bool fresh_db = !FLAGS_use_exist_db;
    bool use_rocksdb = false;

    if (name == "validate") {
      // simplest, create one file with ten keys, and generate it out
      fresh_db = true;
      method = &Benchmark::Validate;
    } else if (name == "merge") {
      // simplest, create one file with ten keys, and generate it out
      fresh_db = true;
      method = &Benchmark::Merge;
    } else if (name == "generate") {
      // simplest, create one file with ten keys, and generate it out
      fresh_db = true;
      method = &Benchmark::Generate;
    } else if (name == "inject") {
      // simplest, create one file with ten keys, and generate it out
      fresh_db = true;
      method = &Benchmark::Inject_LOAD;
    } else if (name == "fillseq") {
      // simplest, create one file with ten keys, and generate it out
      fresh_db = false;
      use_rocksdb = true;
      method = &Benchmark::FillSeq;
    } else if (name == "fillrandom") {
      // simplest, create one file with ten keys, and generate it out
      fresh_db = false;
      use_rocksdb = true;
      method = &Benchmark::FillRandom;
    } else if (name == "fillunique") {
      // simplest, create one file with ten keys, and generate it out
      fresh_db = false;
      use_rocksdb = true;
      method = &Benchmark::FillUnique;
    } else if (name != "") {
      std::cout << "benchmark can't be accept" << std::endl;
      exit(-1);
    }

    if (fresh_db) {
      if (FLAGS_use_exist_db) {
        fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                name.c_str());
        method = nullptr;
      } else {
        if (db_.db != nullptr) {
          db_.DeleteDBs();
          DestroyDB(FLAGS_db_path, open_options_);
        }
        Options options = open_options_;
      }
      int l2_big_tree_num = FLAGS_distinct_num / FLAGS_write_buffer_size;
      int file_num_each_thread = l2_big_tree_num / FLAGS_bench_threads + 1;

      //      Open(&open_options_, use_rocksdb,
      //           FLAGS_bench_threads);  // use open_options for the last
      //           accessed
    }

    if (db_.db == nullptr) {
      Open(&open_options_, use_rocksdb, FLAGS_bench_threads);
    }

    if (method != nullptr) {
      fprintf(stdout, "DB path: [%s]\n", FLAGS_db_path.c_str());
      CombinedStats combined_stats;
      Stats stats = RunBenchmark(FLAGS_bench_threads, name, method);
      combined_stats.AddStats(stats);
    }
  }

  if (secondary_update_thread_) {
    secondary_update_stopped_.store(1, std::memory_order_relaxed);
    secondary_update_thread_->join();
    secondary_update_thread_.reset();
  }
  if (FLAGS_statistics) {
    fprintf(stdout, "STATISTICS:\n%s\n", dbstats->ToString().c_str());
  }
}

void Benchmark::DoWrite(ThreadState* thread, WriteMode write_mode) {
  const int test_duration = write_mode == RANDOM ? FLAGS_duration : 0;
  const int64_t num_ops = FLAGS_distinct_num;

  size_t num_key_gens = 1;

  std::vector<std::unique_ptr<KeyGenerator>> key_gens(num_key_gens);
  int64_t max_ops = num_ops * num_key_gens;
  int64_t ops_per_stage = max_ops;

  Duration duration(test_duration, max_ops, ops_per_stage);
  for (size_t i = 0; i < num_key_gens; i++) {
    if (write_mode == rocksdb::SEQUENTIAL) {
      key_gens[i].reset(new SeqKeyGenerator(FLAGS_min_value));
    } else {
      key_gens[i].reset(new KeyGenerator(&(thread->rand), write_mode,
                                         FLAGS_distinct_num, FLAGS_seed,
                                         FLAGS_key_size, FLAGS_min_value));
    }
  }

  //  Random64 rand_gen(FLAGS_seed);
  //  KeyGenerator key_gen(&rand_gen, SEQUENTIAL, FLAGS_distinct_num,
  //  FLAGS_seed,
  //                       FLAGS_key_size, FLAGS_min_value);

  WriteBatch batch;
  Status s;
  int64_t bytes = 0;

  std::unique_ptr<const char[]> key_guard;
  Slice key = AllocateKey(&key_guard);

  DBImpl* db_ptr = reinterpret_cast<DBImpl*>(db_.db);

  int64_t stage = 0;
  int64_t num_written = 0;
  int current_batch_num = 0;
  while (!duration.Done(1)) {
    if (duration.GetStage() != stage) {
      stage = duration.GetStage();
      if (db_.db != nullptr) {
        db_.CreateNewCf(open_options_, stage);
      }
    }

    size_t id = thread->rand.Next() % num_key_gens;
    DBWithColumnFamilies* db_with_cfh = &db_;
    batch.Clear();
    int64_t batch_bytes = 0;

    int64_t rand_num = key_gens[id]->Next();

    key = key_gens[id]->GenerateKeyFromInt(rand_num);
    Slice val = "valuevalue";

    batch.Put(key, val);

    batch_bytes += val.size() + key_size_;
    bytes += val.size() + key_size_;
    ++num_written;

    current_batch_num++;
    if (current_batch_num > FLAGS_write_batch_size) {
      s = db_with_cfh->db->Write(write_options_, &batch);
      current_batch_num = 0;
    }
    if (thread->shared->write_rate_limiter.get() != nullptr) {
      thread->shared->write_rate_limiter->Request(batch_bytes, Env::IO_HIGH,
                                                  nullptr /* stats */,
                                                  RateLimiter::OpType::kWrite);
      // Set time at which last op finished to Now() to hide latency and
      // sleep from rate limiter. Also, do the check once per batch, not
      // once per write.
      thread->stats.ResetLastOpTime();
    }

    thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db, 1, kWrite);
  }
  assert(s.ok());
  if (!s.ok()) {
    fprintf(stderr, "put error: %s\n", s.ToString().c_str());
    exit(1);
  }

  thread->stats.AddBytes(bytes);
}

}  // namespace gear_db

#endif

//
//  L2SmallTreeCreator l2_small_gen =
//      L2SmallTreeCreator(FLAGS_db_path + "l2_small.sst", basic_options,
//      FLAGS_env,
//                         FLAGS_print_data, FLAGS_delete_new_files);
//  // Prepare the random generators
//  Random64 rand_gen(FLAGS_seed);
//  FLAGS_env->SetBackgroundThreads(FLAGS_max_background_compactions,
//                                  ROCKSDB_NAMESPACE::Env::Priority::LOW);
//  KeyGenerator key_gen(&rand_gen, SEQUENTIAL, FLAGS_distinct_num,
//  FLAGS_seed,
//                       FLAGS_key_size, FLAGS_min_value);
//
//  // Create the mock file generator, estimate the fully compacted l2 big
//  tree MockFileGenerator mock_db(FLAGS_env, FLAGS_db_path, basic_options);
//
//  // Preparation finished
//  std::stringstream benchmark_stream(FLAGS_benchmark);
//  std::string name;
//  while (std::getline(benchmark_stream, name, ',')) {
//    if (name == "merge") {
//      DoMerge(mock_db, &key_gen);
//    } else if (name == "generate") {
//      assert(FLAGS_use_exist_db == false);
//      mock_db.NewDB(FLAGS_use_exist_db);
//      int l2_big_tree_num = FLAGS_distinct_num / FLAGS_write_buffer_size;
//      std::cout << l2_big_tree_num << " SSTs need creatation" << std::endl;
//
//      auto start_ms = FLAGS_env->NowMicros();
//      std::cout << "Start running at: " << start_ms << std::endl;
//      for (int file_num = 0; file_num < l2_big_tree_num; file_num++) {
//        uint64_t smallest_key =
//            file_num * FLAGS_write_buffer_size + FLAGS_min_value;
//        uint64_t largest_key = smallest_key + FLAGS_write_buffer_size - 1;
//        largest_key = std::min(largest_key, FLAGS_distinct_num);
//        mock_db.CreateFileByKeyRange(smallest_key, largest_key, &key_gen);
//        std::cout << "No. " << file_num << " SST generated at "
//                  << FLAGS_env->NowMicros()
//                  << ", smallest key: " << smallest_key
//                  << " largest key: " << largest_key << std::endl;
//      }
//      if (l2_big_tree_num * FLAGS_write_buffer_size < FLAGS_distinct_num) {
//        uint64_t smallest_key =
//            l2_big_tree_num * FLAGS_write_buffer_size + FLAGS_min_value;
//        uint64_t largest_key = smallest_key + FLAGS_write_buffer_size;
//        largest_key = std::min(largest_key, FLAGS_distinct_num);
//        std::string smallest_key_str;
//        std::string largest_key_str;
//        smallest_key_str = key_gen.GenerateKeyFromInt(smallest_key);
//        largest_key_str = key_gen.GenerateKeyFromInt(largest_key);
//
//        mock_db.CreateFileByKeyRange(smallest_key, largest_key, &key_gen);
//        std::cout << "No. " << l2_big_tree_num << " SST generated at "
//                  << FLAGS_env->NowMicros()
//                  << ", smallest key: " << smallest_key
//                  << " largest key: " << largest_key << std::endl;
//      }
//      auto end_micro_sec = FLAGS_env->NowMicros();
//      std::cout << "l2 big tree generating finished at " << end_micro_sec
//                << std::endl;
//
//      std::cout << "Time cost: " << ((double)end_micro_sec - start_ms) /
//      1000000
//                << " sec.";
//      mock_db.FreeDB();
//    } else {
//      mock_db.FreeDB();
//      return -1;
//    }
//  }
//  mock_db.FreeDB();
