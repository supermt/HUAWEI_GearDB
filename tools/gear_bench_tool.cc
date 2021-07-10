//
// Created by jinghuan on 7/6/21.
//
#include <iostream>

#include "rocksdb/gear_bench.h"
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
DEFINE_bool(use_existing_data, true, "Use the existing database or not");
DEFINE_bool(delete_new_files, true, "Delete L2 small tree after bench");
DEFINE_string(db, "", "The database path");
DEFINE_double(span_range, 0.1, "The overlapping range of ");
DEFINE_uint64(distinct_num, 20000000000, "number of distinct entries");
DEFINE_uint64(existing_entries, 80000000000,
              "The number of entries inside existing database, this option "
              "will be ignored while use_existing_data is triggered");
DEFINE_int32(num_column_families, 1, "Number of Column Families to use.");
DEFINE_int32(threads, 1, "Number of concurrent threads to run.");
DEFINE_int32(key_size, 15, "size of each key");
DEFINE_int32(value_size, 10, "size of each value");
DEFINE_uint64(write_buffer_size, 62500000,
              "Size of Memtable, each flush will directly create a l2 small "
              "tree spanning in the entire key space");
DEFINE_uint64(l2_small_size, 62500000, "Size of L2 Small tree");
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
KeyGenerator::KeyGenerator(Random64* rand, WriteMode mode, uint64_t num,
                           uint64_t)
    : rand_(rand), mode_(mode), num_(num), next_(0) {
  if (mode_ == UNIQUE_RANDOM) {
    values_.resize(num_);
    for (uint64_t i = 0; i < num_; ++i) {
      values_[i] = i;
    }
    RandomShuffle(values_.begin(), values_.end(),
                  static_cast<uint32_t>(FLAGS_seed));
  }
}
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
}

void ConfigByGFLAGS(Options& opt) {
  opt.create_if_missing = !FLAGS_use_existing_data;
  opt.env = FLAGS_env;
}
// class BenchMark {
//  private:
//   std::shared_ptr<Cache> cache_;
//   std::shared_ptr<Cache> compressed_cache_;
//   std::shared_ptr<const FilterPolicy> filter_policy_;
//   const SliceTransform* prefix_extractor_;
//   DBWithColumnFamilies db_;
//   std::vector<DBWithColumnFamilies> multi_dbs_;
//   int64_t num_;
//   int key_size_;
//   int prefix_size_;
//   int64_t keys_per_prefix_;
//   int64_t entries_per_batch_;
//   int64_t writes_before_delete_range_;
//   int64_t writes_per_range_tombstone_;
//   int64_t range_tombstone_width_;
//   int64_t max_num_range_tombstones_;
//   WriteOptions write_options_;
//   Options open_options_;  // keep options around to properly destroy db later
//   TraceOptions trace_options_;
//   TraceOptions block_cache_trace_options_;
//   int64_t reads_;
//   int64_t deletes_;
//   double read_random_exp_range_;
//   int64_t writes_;
//   int64_t readwrites_;
//   int64_t merge_keys_;
//   bool report_file_operations_;
//   bool use_blob_db_;
//   std::vector<std::string> keys_;
//
//   class ErrorHandlerListener : public EventListener {
//    public:
//#ifndef ROCKSDB_LITE
//     ErrorHandlerListener()
//         : mutex_(),
//           cv_(&mutex_),
//           no_auto_recovery_(false),
//           recovery_complete_(false) {}
//
//     ~ErrorHandlerListener() override {}
//
//     void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/,
//                               Status /*bg_error*/,
//                               bool* auto_recovery) override {
//       if (*auto_recovery && no_auto_recovery_) {
//         *auto_recovery = false;
//       }
//     }
//
//     void OnErrorRecoveryCompleted(Status /*old_bg_error*/) override {
//       InstrumentedMutexLock l(&mutex_);
//       recovery_complete_ = true;
//       cv_.SignalAll();
//     }
//
//     bool WaitForRecovery(uint64_t abs_time_us) {
//       InstrumentedMutexLock l(&mutex_);
//       if (!recovery_complete_) {
//         cv_.TimedWait(abs_time_us);
//       }
//       if (recovery_complete_) {
//         recovery_complete_ = false;
//         return true;
//       }
//       return false;
//     }
//
//     void EnableAutoRecovery(bool enable = true) { no_auto_recovery_ =
//     !enable; }
//
//    private:
//     InstrumentedMutex mutex_;
//     InstrumentedCondVar cv_;
//     bool no_auto_recovery_;
//     bool recovery_complete_;
//#else   // ROCKSDB_LITE
//     bool WaitForRecovery(uint64_t /*abs_time_us*/) { return true; }
//     void EnableAutoRecovery(bool /*enable*/) {}
//#endif  // ROCKSDB_LITE
//   };
//
//   std::shared_ptr<ErrorHandlerListener> listener_;
//
//  public:
//   void GenerateDB(Options& opt) {
//     if (!FLAGS_use_existing_data) {
//       DestroyDB(FLAGS_db, opt);  // destroy target
//       FLAGS_env->CreateDirIfMissing(FLAGS_db);
//     }
//     listener_.reset(new ErrorHandlerListener());
//   }
// };

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
std::string GenerateKeyFromInt(uint64_t v, int64_t num_keys, Slice* key) {
  v = v % num_keys;
  char* start = const_cast<char*>(key->data());
  char* pos = start;
  int bytes_to_fill =
      std::min(FLAGS_key_size - static_cast<int>(pos - start), 8);
  if (port::kLittleEndian) {
    for (int i = 0; i < bytes_to_fill; ++i) {
      pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
    }
  } else {
    memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
  }
  pos += bytes_to_fill;
  if (FLAGS_key_size > pos - start) {
    memset(pos, '0', FLAGS_key_size - (pos - start));
  }
  return key->ToString();
}

class L2SmallTreeCreator {
 protected:
  Options options_;
  EnvOptions soptions_;
  std::string sst_name_;
  std::shared_ptr<Env> env_guard_;
  Env* env_;

 public:
  L2SmallTreeCreator(std::string sst_name, Options& options) {
    options_ = options;
    sst_name_ = sst_name;
    Env* base_env = FLAGS_env;
    env_ = base_env;
    options_.env = env_;
  }

  ~L2SmallTreeCreator() {
    if (FLAGS_delete_new_files) {
      Status s = env_->DeleteFile(sst_name_);
    }
  }

  void CreateFile(const std::string& file_name,
                  const std::vector<std::string>& keys) {
    SstFileWriter writer(soptions_, options_);
    writer.Open(file_name);
    std::string value = "1234567890";
    for (size_t i = 0; i < keys.size(); i++) {
      std::cout << keys[i] << std::endl;
      writer.Put(keys[i], value);
    }
    std::cout << file_name << std::endl;
    writer.Finish();
  }

  void CheckFile(const std::string& file_name,
                 const std::vector<std::string>& keys) {
    ReadOptions ropts;
    SstFileReader reader(options_);
    assert(reader.Open(file_name).ok());
    std::unique_ptr<Iterator> iter(reader.NewIterator(ropts));
    iter->SeekToFirst();
    for (size_t i = 0; i < keys.size(); i++) {
      assert(iter->Valid());
      if (FLAGS_print_data) {
        std::cout << "Key: " << iter->key().ToString(true) << std::endl;
      }
      assert(iter->key().ToString() == keys[i]);
      iter->Next();
    }
  }

  void CreateFileAndCheck(const std::vector<std::string>& keys) {
    CreateFile(sst_name_, keys);
    CheckFile(sst_name_, keys);
  }
};
Slice AllocateKey(std::unique_ptr<const char[]>* key_guard) {
  char* data = new char[FLAGS_key_size];
  const char* const_data = data;
  key_guard->reset(const_data);
  return Slice(key_guard->get(), FLAGS_key_size);
}
int gear_bench(int argc, char** argv) {
  //
  ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_db.back() != '/') {
    FLAGS_db.push_back('/');
  }
  std::cout << "db at " << FLAGS_db << std::endl;
  Options basic_options;
  basic_options.create_if_missing = !FLAGS_use_existing_data;
  basic_options.db_paths.emplace_back(FLAGS_db, 100000);
  basic_options.index_dir_prefix = FLAGS_index_dir_prefix;
  constant_options(basic_options);
  L2SmallTreeCreator l2_gen =
      L2SmallTreeCreator(FLAGS_db + "/test.sst", basic_options);
  Random64 rand_gen(FLAGS_seed);
  KeyGenerator key_gen(&rand_gen, SEQUENTIAL, 100, 50 * 10000);
  uint64_t next_data = key_gen.Next();
  std::unique_ptr<const char[]> key_guard;
  Slice key = AllocateKey(&key_guard);

  std::vector<std::string> keys;
  std::string temp;
  for (uint64_t i = 0; i < 10; i++) {
    temp = GenerateKeyFromInt(i, FLAGS_distinct_num, &key);
    //    std::cout << key.ToString(true) << std::endl;
    keys.push_back(temp);
  }
  l2_gen.CreateFileAndCheck(keys);
  // should we really open a SSTable?

  return 0;
}

}  // namespace ROCKSDB_NAMESPACE
#endif