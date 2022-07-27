//
// Created by jinghuan on 7/12/21.
//
#pragma once
#include <algorithm>
#include <array>
#include <cinttypes>
#include <iostream>
#include <map>
#include <string>
#include <tuple>

#include "db/column_family.h"
#include "db/compaction/compaction_job.h"
#include "db/compaction/compaction_picker_gear.h"
#include "db/db_impl/db_impl.h"
#include "db/error_handler.h"
#include "db/version_set.h"
#include "env/composite_env_wrapper.h"
#include "file/writable_file_writer.h"
#include "gear_bench_classes.h"
#include "options.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/utilities/sim_cache.h"
#include "rocksdb/write_buffer_manager.h"
#include "sst_file_reader.h"
#include "utilities/DOTA/report_agent.h"

namespace ROCKSDB_NAMESPACE {
static std::string ColumnFamilyName(size_t i) {
  if (i == 0) {
    return kDefaultColumnFamilyName;
  } else {
    char name[100];
    snprintf(name, sizeof(name), "column_family_name_%06zu", i);
    return std::string(name);
  }
}

enum WriteMode { RANDOM, SEQUENTIAL, UNIQUE_RANDOM };

class KeyGenerator {
 public:
  KeyGenerator(Random64* rand, WriteMode mode, uint64_t num, uint64_t seed,
               int key_size, uint64_t min_value);
  virtual ~KeyGenerator();

  virtual uint64_t Next() {
    switch (mode_) {
      case SEQUENTIAL:
        return next_++;
      case RANDOM:
        return rand_->Next() % distinct_num_;
      case UNIQUE_RANDOM:
        assert(next_ < distinct_num_);
        return values_[next_++];
    }
    assert(false);
  }

  void SetNext(uint64_t next) { next_ = next; }
  std::string NextString() {
    return GenerateKeyFromInt(Next(), distinct_num_, &key_slice_, key_size_);
  }
  std::string GenerateKeyFromInt(uint64_t v) {
    return GenerateKeyFromInt(v, distinct_num_, &key_slice_, key_size_);
  }
  static std::string GenerateKeyFromInt(uint64_t v, uint64_t num_keys,
                                        Slice* key, int key_size = 15);
  static Slice AllocateKey(std::unique_ptr<const char[]>* key_guard,
                           int key_size = 15);

 protected:
  Random64* rand_;
  WriteMode mode_;
  uint64_t min_;
  const uint64_t distinct_num_;
  uint64_t next_;
  int key_size_ = 16;
  std::vector<uint64_t> values_;
  Slice key_slice_;
  std::unique_ptr<const char[]> key_guard;
};
class SeqKeyGenerator : public KeyGenerator {
 public:
  explicit SeqKeyGenerator(uint64_t min_value)
      : KeyGenerator(nullptr, SEQUENTIAL, std::pow(256, 7), 0, 8, min_value),
        seq_no(0){};

  uint64_t Next() override;

 private:
  std::atomic_uint64_t seq_no;
};

struct DBWithColumnFamilies {
  std::vector<ColumnFamilyHandle*> cfh;
  DB* db;
  std::atomic<size_t> num_created;  // Need to be updated after all the
  // new entries in cfh are set.
  size_t num_hot;  // Number of column families to be queried at each moment.
  // After each CreateNewCf(), another num_hot number of new
  // Column families will be created and used to be queried.
  port::Mutex create_cf_mutex;  // Only one thread can execute CreateNewCf()
  std::vector<int> cfh_idx_to_prob;  // ith index holds probability of operating
  // on cfh[i].

  DBWithColumnFamilies() : db(nullptr) {
    cfh.clear();
    num_created = 0;
    num_hot = 0;
  }

  DBWithColumnFamilies(const DBWithColumnFamilies& other)
      : cfh(other.cfh),
        db(other.db),
        num_created(other.num_created.load()),
        num_hot(other.num_hot),
        cfh_idx_to_prob(other.cfh_idx_to_prob) {}

  void DeleteDBs() {
    std::for_each(cfh.begin(), cfh.end(),
                  [](ColumnFamilyHandle* cfhi) { delete cfhi; });
    cfh.clear();
    delete db;
    db = nullptr;
  }

  ColumnFamilyHandle* GetCfh(int64_t rand_num) {
    assert(num_hot > 0);
    size_t rand_offset = 0;
    if (!cfh_idx_to_prob.empty()) {
      assert(cfh_idx_to_prob.size() == num_hot);
      int sum = 0;
      while (sum + cfh_idx_to_prob[rand_offset] < rand_num % 100) {
        sum += cfh_idx_to_prob[rand_offset];
        ++rand_offset;
      }
      assert(rand_offset < cfh_idx_to_prob.size());
    } else {
      rand_offset = rand_num % num_hot;
    }
    return cfh[num_created.load(std::memory_order_acquire) - num_hot +
               rand_offset];
  }

  // stage: assume CF from 0 to stage * num_hot has be created. Need to create
  //        stage * num_hot + 1 to stage * (num_hot + 1).
  void CreateNewCf(ColumnFamilyOptions options, int64_t stage) {
    MutexLock l(&create_cf_mutex);
    if ((stage + 1) * num_hot <= num_created) {
      // Already created.
      return;
    }
    auto new_num_created = num_created + num_hot;
    assert(new_num_created <= cfh.size());
    for (size_t i = num_created; i < new_num_created; i++) {
      Status s =
          db->CreateColumnFamily(options, ColumnFamilyName(i), &(cfh[i]));
      if (!s.ok()) {
        fprintf(stderr, "create column family error: %s\n",
                s.ToString().c_str());
        abort();
      }
    }
    num_created.store(new_num_created, std::memory_order_release);
  }
};

class L2SmallTreeCreator {
 protected:
  Options options_;
  EnvOptions envOptions;
  std::string sst_name_;
  std::shared_ptr<Env> env_guard_;
  Env* env_;
  bool print_data_;
  bool delete_new_files_;

 public:
  L2SmallTreeCreator(std::string sst_name, Options& options, Env* env,
                     bool print_data, bool delete_new_files) {
    options_ = options;
    sst_name_ = sst_name;
    Env* base_env = env;
    env_ = base_env;
    options_.env = env_;
    print_data_ = print_data;
    delete_new_files_ = delete_new_files;
  }

  ~L2SmallTreeCreator() {
    if (delete_new_files_) {
      Status s = env_->DeleteFile(sst_name_);
    }
  }
  void CreateFile(const std::vector<std::string>& keys) {
    SstFileWriter writer(envOptions, options_);
    writer.Open(this->sst_name_);
    std::string value = "1234567890";
    for (size_t i = 0; i < keys.size(); i++) {
      std::cout << keys[i] << std::endl;
      writer.Put(keys[i], value);
    }
    std::cout << "Write in " << sst_name_ << " finished " << std::endl;
    writer.Finish();
  }
  void CreateFile(const stl_wrappers::KVMap& kvmap) {
    SstFileWriter writer(envOptions, options_);
    writer.Open(this->sst_name_);
    for (auto entry : kvmap) {
      writer.Put(entry.first, entry.second);
    }
    std::cout << "Write in " << sst_name_ << " finished" << std::endl;
  }
  void CreateFile(const std::string& file_name,
                  const std::vector<std::string>& keys) {
    SstFileWriter writer(envOptions, options_);
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
    reader.Open(file_name);
    std::unique_ptr<Iterator> iter(reader.NewIterator(ropts));
    iter->SeekToFirst();
    for (size_t i = 0; i < keys.size(); i++) {
      assert(iter->Valid());
      if (print_data_) {
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

class MockFileGenerator {
 public:
  // Member fields
  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  std::string dbname_;
  EnvOptions env_options_;
  ImmutableDBOptions db_options_;
  ColumnFamilyOptions cf_options_;
  Options options_;
  MutableCFOptions mutable_cf_options_;
  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  WriteBufferManager write_buffer_manager_;
  std::unique_ptr<VersionSet> versions_;
  InstrumentedMutex mutex_;
  std::atomic<bool> shutting_down_;
  SequenceNumber preserve_deletes_seqnum_;
  std::shared_ptr<TableFactory> gear_table_factory;
  CompactionJobStats compaction_job_stats_;
  ColumnFamilyData* cfd_;
  std::unique_ptr<CompactionFilter> compaction_filter_;
  std::shared_ptr<MergeOperator> merge_op_;
  std::vector<std::unique_ptr<SstFileWriter>> writers_;
  std::vector<int> file_numbers_;
  //  std::unique_ptr<SstFileWriter> writer_;
  ErrorHandler error_handler_;
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  const int bench_threads_;
  // function field.
  MockFileGenerator(Env* env, const std::string& db_name, Options& opt,
                    int bench_threads)
      : env_(env),
        fs_(std::make_shared<LegacyFileSystemWrapper>(env_)),
        dbname_(db_name),
        db_options_(opt),
        cf_options_(opt),
        options_(opt),
        mutable_cf_options_(opt),
        table_cache_(NewLRUCache(50000, 16)),
        write_buffer_manager_(db_options_.db_write_buffer_size),
        versions_(new VersionSet(dbname_, &db_options_, env_options_,
                                 table_cache_.get(), &write_buffer_manager_,
                                 &write_controller_, nullptr)),
        shutting_down_(false),
        preserve_deletes_seqnum_(0),
        writers_(bench_threads),
        file_numbers_(bench_threads),
        error_handler_(nullptr, db_options_, &mutex_),
        ucmp_(BytewiseComparator()),
        icmp_(ucmp_),
        bench_threads_(bench_threads) {
    auto mk_result = system(("mkdir -p " + db_name).c_str());
    if (mk_result) exit(-1);
    env_->CreateDirIfMissing(db_name);
    env_->CreateDir(db_name + opt.index_dir_prefix);
    env_->SetBackgroundThreads(db_options_.max_subcompactions);
    db_options_.env = env_;
    db_options_.fs = fs_;
    //    GearTableOptions gearTableOptions;
    //    gearTableOptions.encoding_type = kPlain;
    //    gearTableOptions.user_key_len = 15;
    //    gearTableOptions.user_value_len = 10;
    gear_table_factory = opt.table_factory;
    CreateLoggerFromOptions(db_name, opt, &opt.info_log);
    assert(!db_options_.db_paths.empty());
  }
  std::string GenerateFileName(uint64_t file_number) {
    FileMetaData meta;
    meta.fd = FileDescriptor(file_number, 0, 0);
    return TableFileName(this->db_options_.db_paths, meta.fd.GetNumber(),
                         meta.fd.GetPathId());
  }
  void PrintFullTree(ColumnFamilyData* cfd) {
    for (int i = 0; i < 3; i++) {
      std::cout << "level: " << i << " : " << std::endl;
      for (auto file : cfd->current()->storage_info()->LevelFiles(i)) {
        std::cout << "File Number: " << file->fd.GetNumber()
                  << ", file's smallest key: "
                  << file->smallest.user_key().ToString(true)
                  << ", file's largest key: "
                  << file->smallest.user_key().ToString(true)
                  << ", l2 potions: " << file->l2_position << std::endl;
      }
    }
  }
  static std::string KeyStr(const std::string& user_key,
                            const SequenceNumber seq_num, const ValueType t) {
    return InternalKey(user_key, seq_num, t).Encode().ToString();
  }

  void SetLastSequence(const SequenceNumber sequence_number) {
    versions_->SetLastAllocatedSequence(sequence_number + 1);
    versions_->SetLastPublishedSequence(sequence_number + 1);
    versions_->SetLastSequence(sequence_number + 1);
  }

  Status AddMockFile(const stl_wrappers::KVMap& contents, int level,
                     int l2_position, int thread_id);
  Status AddMockFile(uint64_t start_num, uint64_t end_num,
                     KeyGenerator* key_gen, SequenceNumber start_seq,
                     int thread_id, int level = 2,
                     int l2_position = l2_large_tree_index);
  Status TriggerCompaction();
  void NewDB(bool use_existing_data);
  void FreeDB();
  Status CreateFileByKeyRange(uint64_t smallest_key, uint64_t largest_key,
                              KeyGenerator* key_gen, int thread_id,
                              SequenceNumber start_seq = 0);
  void ReOpenDB();
};

class SpanningKeyGenerator {
  uint64_t lower_bound_;
  uint64_t upper_bound_;
  std::set<uint64_t> result_list;
  std::default_random_engine engine;

 public:
  enum DISTRIBUTION_TYPE { kUniform, kOther };

 public:
  SpanningKeyGenerator(uint64_t lower_bound, uint64_t upper_bound, int num_keys,
                       int seed = 0, DISTRIBUTION_TYPE distribution = kUniform)
      : lower_bound_(lower_bound), upper_bound_(upper_bound), engine(seed) {
    assert(lower_bound_ < upper_bound_);
    switch (distribution) {
      case kUniform: {
        std::uniform_int_distribution<uint64_t> distributor(lower_bound_,
                                                            upper_bound_);
        for (int i = 0; i < num_keys; i++) {
          result_list.insert(distributor(engine));
        }
        break;
      }
      case kOther:
        result_list.clear();
        break;
    }
  }
  stl_wrappers::KVMap GenerateContent(KeyGenerator* key_gen_ptr,
                                      SequenceNumber* seqno) {
    stl_wrappers::KVMap content;
    std::string value = "1234567890";
    for (auto key : result_list) {
      InternalKey ikey(key_gen_ptr->GenerateKeyFromInt(key), ++(*seqno),
                       kTypeValue);
      *seqno += 1;
      content.emplace(ikey.Encode().ToString(), value);
    }
    return content;
  }
};

namespace {
struct ReportFileOpCounters {
  std::atomic<int> open_counter_;
  std::atomic<int> read_counter_;
  std::atomic<int> append_counter_;
  std::atomic<uint64_t> bytes_read_;
  std::atomic<uint64_t> bytes_written_;
};

// A special Env to records and report file operations in db_bench
class ReportFileOpEnv : public rocksdb::EnvWrapper {
 public:
  explicit ReportFileOpEnv(Env* base) : EnvWrapper(base) { reset(); }

  void reset() {
    counters_.open_counter_ = 0;
    counters_.read_counter_ = 0;
    counters_.append_counter_ = 0;
    counters_.bytes_read_ = 0;
    counters_.bytes_written_ = 0;
  }

  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& soptions) override {
    class CountingFile : public SequentialFile {
     private:
      std::unique_ptr<SequentialFile> target_;
      ReportFileOpCounters* counters_;

     public:
      CountingFile(std::unique_ptr<SequentialFile>&& target,
                   ReportFileOpCounters* counters)
          : target_(std::move(target)), counters_(counters) {}

      Status Read(size_t n, Slice* result, char* scratch) override {
        counters_->read_counter_.fetch_add(1, std::memory_order_relaxed);
        Status rv = target_->Read(n, result, scratch);
        counters_->bytes_read_.fetch_add(result->size(),
                                         std::memory_order_relaxed);
        return rv;
      }

      Status Skip(uint64_t n) override { return target_->Skip(n); }
    };

    Status s = target()->NewSequentialFile(f, r, soptions);
    if (s.ok()) {
      counters()->open_counter_.fetch_add(1, std::memory_order_relaxed);
      r->reset(new CountingFile(std::move(*r), counters()));
    }
    return s;
  }

  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& soptions) override {
    class CountingFile : public RandomAccessFile {
     private:
      std::unique_ptr<RandomAccessFile> target_;
      ReportFileOpCounters* counters_;

     public:
      CountingFile(std::unique_ptr<RandomAccessFile>&& target,
                   ReportFileOpCounters* counters)
          : target_(std::move(target)), counters_(counters) {}
      Status Read(uint64_t offset, size_t n, Slice* result,
                  char* scratch) const override {
        counters_->read_counter_.fetch_add(1, std::memory_order_relaxed);
        Status rv = target_->Read(offset, n, result, scratch);
        counters_->bytes_read_.fetch_add(result->size(),
                                         std::memory_order_relaxed);
        return rv;
      }
    };

    Status s = target()->NewRandomAccessFile(f, r, soptions);
    if (s.ok()) {
      counters()->open_counter_.fetch_add(1, std::memory_order_relaxed);
      r->reset(new CountingFile(std::move(*r), counters()));
    }
    return s;
  }

  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& soptions) override {
    class CountingFile : public WritableFile {
     private:
      std::unique_ptr<WritableFile> target_;
      ReportFileOpCounters* counters_;

     public:
      CountingFile(std::unique_ptr<WritableFile>&& target,
                   ReportFileOpCounters* counters)
          : target_(std::move(target)), counters_(counters) {}

      Status Append(const Slice& data) override {
        counters_->append_counter_.fetch_add(1, std::memory_order_relaxed);
        Status rv = target_->Append(data);
        counters_->bytes_written_.fetch_add(data.size(),
                                            std::memory_order_relaxed);
        return rv;
      }

      Status Truncate(uint64_t size) override {
        return target_->Truncate(size);
      }
      Status Close() override { return target_->Close(); }
      Status Flush() override { return target_->Flush(); }
      Status Sync() override { return target_->Sync(); }
    };

    Status s = target()->NewWritableFile(f, r, soptions);
    if (s.ok()) {
      counters()->open_counter_.fetch_add(1, std::memory_order_relaxed);
      r->reset(new CountingFile(std::move(*r), counters()));
    }
    return s;
  }

  // getter
  ReportFileOpCounters* counters() { return &counters_; }

 private:
  ReportFileOpCounters counters_;
};

}  // namespace

}  // namespace ROCKSDB_NAMESPACE

namespace gear_db {
using namespace rocksdb;
static int64_t Default_cache_size = 8 << 20;
static int32_t Default_cache_numshardbits = 6;
static int64_t Default_simcache_size = -1;
static int32_t Default_ops_between_duration_checks = 1000;
static int64_t Default_row_cache_size = 0;
static int32_t Default_bloom_bits = -1;
static bool Default_use_existing_db = false;
static bool Default_show_table_properties = false;
static std::string Default_db = "";
static class std::shared_ptr<ROCKSDB_NAMESPACE::Statistics> dbstats;
static std::string Default_wal_dir = "";
static int32_t Default_num_levels = 7;
static int32_t Default_compression_level =
    ROCKSDB_NAMESPACE::CompressionOptions().level;
static int32_t Default_compression_max_dict_bytes =
    ROCKSDB_NAMESPACE::CompressionOptions().max_dict_bytes;
static int32_t Default_compression_zstd_max_train_bytes =
    ROCKSDB_NAMESPACE::CompressionOptions().zstd_max_train_bytes;
static int32_t Default_compression_parallel_threads = 1;
static ROCKSDB_NAMESPACE::Env* Default_env = ROCKSDB_NAMESPACE::Env::Default();
static int64_t Default_stats_interval = 0;
static int64_t Default_stats_interval_seconds = 0;
static int32_t Default_stats_per_interval = 0;
static int64_t Default_report_interval_seconds = 1;
static std::string Default_report_file = "report.csv";
static int32_t Default_thread_status_per_interval = 0;
static int32_t Default_perf_level = ROCKSDB_NAMESPACE::PerfLevel::kDisable;
static int32_t Default_prefix_size = 0;
static bool Default_enable_io_prio = false;
static bool Default_enable_cpu_prio = false;
static bool Default_dump_malloc_stats = true;
static uint64_t Default_stats_dump_period_sec =
    ROCKSDB_NAMESPACE::Options().stats_dump_period_sec;
static uint64_t Default_stats_persist_period_sec =
    ROCKSDB_NAMESPACE::Options().stats_persist_period_sec;
static bool Default_persist_stats_to_disk =
    ROCKSDB_NAMESPACE::Options().persist_stats_to_disk;
static uint64_t Default_stats_history_buffer_size =
    ROCKSDB_NAMESPACE::Options().stats_history_buffer_size;
static bool Default_use_block_based_filter = false;
static bool Default_report_file_operations = false;
static bool Default_report_thread_idle = false;
static int Default_seed = 0;

enum OperationType : unsigned char {
  kRead = 0,
  kWrite,
  kDelete,
  kSeek,
  kMerge,
  kUpdate,
  kCompress,
  kUncompress,
  kCrc,
  kHash,
  kOthers
};

static std::unordered_map<OperationType, std::string, std::hash<unsigned char>>
    OperationTypeString = {{kRead, "read"},         {kWrite, "write"},
                           {kDelete, "delete"},     {kSeek, "seek"},
                           {kMerge, "merge"},       {kUpdate, "update"},
                           {kCompress, "compress"}, {kCompress, "uncompress"},
                           {kCrc, "crc"},           {kHash, "hash"},
                           {kOthers, "op"}};

class Stats {
 private:
  int id_;
  uint64_t start_ = 0;
  uint64_t sine_interval_;
  uint64_t finish_;
  double seconds_;
  uint64_t done_;
  uint64_t last_report_done_;
  uint64_t next_report_;
  uint64_t bytes_;
  uint64_t last_op_finish_;
  uint64_t last_report_finish_;
  std::unordered_map<OperationType, std::shared_ptr<HistogramImpl>,
                     std::hash<unsigned char>>
      hist_;
  std::string message_;
  bool exclude_from_merge_;
  rocksdb::ReporterAgent* reporter_agent_;  // does not own
  friend class CombinedStats;

 public:
  Stats() { Start(-1); }

  void SetReporterAgent(ReporterAgent* reporter_agent) {
    reporter_agent_ = reporter_agent;
  }

  void Start(int id) {
    id_ = id;
    next_report_ = Default_stats_interval ? Default_stats_interval : 100;
    last_op_finish_ = start_;
    hist_.clear();
    done_ = 0;
    last_report_done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = Default_env->NowMicros();
    sine_interval_ = Default_env->NowMicros();
    finish_ = start_;
    last_report_finish_ = start_;
    message_.clear();
    // When set, stats from this thread won't be merged with others.
    exclude_from_merge_ = false;
  }

  void Merge(const Stats& other) {
    if (other.exclude_from_merge_) return;

    for (auto it = other.hist_.begin(); it != other.hist_.end(); ++it) {
      auto this_it = hist_.find(it->first);
      if (this_it != hist_.end()) {
        this_it->second->Merge(*(other.hist_.at(it->first)));
      } else {
        hist_.insert({it->first, it->second});
      }
    }

    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = Default_env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }
  static void AppendWithSpace(std::string* str, Slice msg) {
    if (msg.empty()) return;
    if (!str->empty()) {
      str->push_back(' ');
    }
    str->append(msg.data(), msg.size());
  }
  void AddMessage(Slice msg) { AppendWithSpace(&message_, msg); }

  void SetId(int id) { id_ = id; }
  void SetExcludeFromMerge() { exclude_from_merge_ = true; }

  void PrintThreadStatus() {
    std::vector<ThreadStatus> thread_list;
    Default_env->GetThreadList(&thread_list);

    fprintf(stderr, "\n%18s %10s %12s %20s %13s %45s %12s %s\n", "ThreadID",
            "ThreadType", "cfName", "Operation", "ElapsedTime", "Stage",
            "State", "OperationProperties");

    int64_t current_time = 0;
    Default_env->GetCurrentTime(&current_time);
    for (auto ts : thread_list) {
      fprintf(stderr, "%18" PRIu64 " %10s %12s %20s %13s %45s %12s",
              ts.thread_id,
              ThreadStatus::GetThreadTypeName(ts.thread_type).c_str(),
              ts.cf_name.c_str(),
              ThreadStatus::GetOperationName(ts.operation_type).c_str(),
              ThreadStatus::MicrosToString(ts.op_elapsed_micros).c_str(),
              ThreadStatus::GetOperationStageName(ts.operation_stage).c_str(),
              ThreadStatus::GetStateName(ts.state_type).c_str());

      auto op_properties = ThreadStatus::InterpretOperationProperties(
          ts.operation_type, ts.op_properties);
      for (const auto& op_prop : op_properties) {
        fprintf(stderr, " %s %" PRIu64 " |", op_prop.first.c_str(),
                op_prop.second);
      }
      fprintf(stderr, "\n");
    }
  }

  void ResetSineInterval() { sine_interval_ = Default_env->NowMicros(); }

  uint64_t GetSineInterval() { return sine_interval_; }

  uint64_t GetStart() { return start_; }

  void ResetLastOpTime() {
    // Set to now to avoid latency from calls to SleepForMicroseconds
    last_op_finish_ = Default_env->NowMicros();
  }

  void FinishedOps(DBWithColumnFamilies* db_with_cfh, DB* db, int64_t num_ops,
                   enum OperationType op_type = kOthers) {
    if (reporter_agent_) {
      reporter_agent_->ReportFinishedOps(num_ops);
    }

    done_ += num_ops;
    if (done_ >= next_report_) {
      if (!Default_stats_interval) {
        if (next_report_ < 1000)
          next_report_ += 100;
        else if (next_report_ < 5000)
          next_report_ += 500;
        else if (next_report_ < 10000)
          next_report_ += 1000;
        else if (next_report_ < 50000)
          next_report_ += 5000;
        else if (next_report_ < 100000)
          next_report_ += 10000;
        else if (next_report_ < 500000)
          next_report_ += 50000;
        else
          next_report_ += 100000;
        fprintf(stderr, "... finished %" PRIu64 " ops%30s\r", done_, "");
      } else {
        uint64_t now = Default_env->NowMicros();
        int64_t usecs_since_last = now - last_report_finish_;

        // Determine whether to print status where interval is either
        // each N operations or each N seconds.

        if (Default_stats_interval_seconds &&
            usecs_since_last < (Default_stats_interval_seconds * 1000000)) {
          // Don't check again for this many operations
          next_report_ += Default_stats_interval;

        } else {
          fprintf(stderr,
                  "%s ... thread %d: (%" PRIu64 ",%" PRIu64
                  ") ops and "
                  "(%.1f,%.1f) ops/second in (%.6f,%.6f) seconds\n",
                  Default_env->TimeToString(now / 1000000).c_str(), id_,
                  done_ - last_report_done_, done_,
                  (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
                  done_ / ((now - start_) / 1000000.0),
                  (now - last_report_finish_) / 1000000.0,
                  (now - start_) / 1000000.0);

          if (id_ == 0 && Default_stats_per_interval) {
            std::string stats;

            if (db_with_cfh && db_with_cfh->num_created.load()) {
              for (size_t i = 0; i < db_with_cfh->num_created.load(); ++i) {
                if (db->GetProperty(db_with_cfh->cfh[i], "rocksdb.cfstats",
                                    &stats))
                  fprintf(stderr, "%s\n", stats.c_str());
                if (Default_show_table_properties) {
                  for (int level = 0; level < Default_num_levels; ++level) {
                    if (db->GetProperty(
                            db_with_cfh->cfh[i],
                            "rocksdb.aggregated-table-properties-at-level" +
                                ToString(level),
                            &stats)) {
                      if (stats.find("# entries=0") == std::string::npos) {
                        fprintf(stderr, "Level[%d]: %s\n", level,
                                stats.c_str());
                      }
                    }
                  }
                }
              }
            } else if (db) {
              if (db->GetProperty("rocksdb.stats", &stats)) {
                fprintf(stderr, "%s\n", stats.c_str());
              }
              if (Default_show_table_properties) {
                for (int level = 0; level < Default_num_levels; ++level) {
                  if (db->GetProperty(
                          "rocksdb.aggregated-table-properties-at-level" +
                              ToString(level),
                          &stats)) {
                    if (stats.find("# entries=0") == std::string::npos) {
                      fprintf(stderr, "Level[%d]: %s\n", level, stats.c_str());
                    }
                  }
                }
              }
            }
          }

          next_report_ += Default_stats_interval;
          last_report_finish_ = now;
          last_report_done_ = done_;
        }
      }
      if (id_ == 0 && Default_thread_status_per_interval) {
        PrintThreadStatus();
      }
      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) { bytes_ += n; }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedOps().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);
    double elapsed = (finish_ - start_) * 1e-6;
    double throughput = (double)done_ / elapsed;

    fprintf(stdout, "%-12s : %11.3f micros/op %ld ops/sec;%s%s\n",
            name.ToString().c_str(), seconds_ * 1e6 / done_, (long)throughput,
            (extra.empty() ? "" : " "), extra.c_str());

    if (Default_report_thread_idle) {
      std::string result_string = Default_env->GetThreadPoolTimeStateString();
      std::cout << result_string;
    }
    if (Default_report_file_operations) {
      ReportFileOpEnv* env = static_cast<ReportFileOpEnv*>(Default_env);
      ReportFileOpCounters* counters = env->counters();
      fprintf(stdout, "Num files opened: %d\n",
              counters->open_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num Read(): %d\n",
              counters->read_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num Append(): %d\n",
              counters->append_counter_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num bytes read: %" PRIu64 "\n",
              counters->bytes_read_.load(std::memory_order_relaxed));
      fprintf(stdout, "Num bytes written: %" PRIu64 "\n",
              counters->bytes_written_.load(std::memory_order_relaxed));
      env->reset();
    }
    fflush(stdout);
  }
};

class CombinedStats {
 public:
  void AddStats(const Stats& stat) {
    uint64_t total_ops = stat.done_;
    uint64_t total_bytes_ = stat.bytes_;
    double elapsed;

    if (total_ops < 1) {
      total_ops = 1;
    }

    elapsed = (stat.finish_ - stat.start_) * 1e-6;
    throughput_ops_.emplace_back(total_ops / elapsed);

    if (total_bytes_ > 0) {
      double mbs = (total_bytes_ / 1048576.0);
      throughput_mbs_.emplace_back(mbs / elapsed);
    }
  }

  void Report(const std::string& bench_name) {
    const char* name = bench_name.c_str();
    int num_runs = static_cast<int>(throughput_ops_.size());

    if (throughput_mbs_.size() == throughput_ops_.size()) {
      fprintf(stdout,
              "%s [AVG    %d runs] : %d ops/sec; %6.1f MB/sec\n"
              "%s [MEDIAN %d runs] : %d ops/sec; %6.1f MB/sec\n",
              name, num_runs, static_cast<int>(CalcAvg(throughput_ops_)),
              CalcAvg(throughput_mbs_), name, num_runs,
              static_cast<int>(CalcMedian(throughput_ops_)),
              CalcMedian(throughput_mbs_));
    } else {
      fprintf(stdout,
              "%s [AVG    %d runs] : %d ops/sec\n"
              "%s [MEDIAN %d runs] : %d ops/sec\n",
              name, num_runs, static_cast<int>(CalcAvg(throughput_ops_)), name,
              num_runs, static_cast<int>(CalcMedian(throughput_ops_)));
    }
  }

 private:
  double CalcAvg(std::vector<double> data) {
    double avg = 0;
    for (double x : data) {
      avg += x;
    }
    avg = avg / data.size();
    return avg;
  }

  double CalcMedian(std::vector<double> data) {
    assert(data.size() > 0);
    std::sort(data.begin(), data.end());

    size_t mid = data.size() / 2;
    if (data.size() % 2 == 1) {
      // Odd number of entries
      return data[mid];
    } else {
      // Even number of entries
      return (data[mid] + data[mid - 1]) / 2;
    }
  }

  std::vector<double> throughput_ops_;
  std::vector<double> throughput_mbs_;
};
struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;
  int perf_level;
  std::shared_ptr<RateLimiter> write_rate_limiter;
  std::shared_ptr<RateLimiter> read_rate_limiter;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  long num_initialized;
  long num_done;
  bool start;

  SharedState() : cv(&mu), perf_level(Default_perf_level) {}
};

struct ThreadState {
  int tid;        // 0..n-1 when running in n threads
  Random64 rand;  // Has different seeds for different threads
  Stats stats;
  SharedState* shared;

  /* implicit */ ThreadState(int index)
      : tid(index), rand((Default_seed ? Default_seed : 1000) + index) {}
};

class Duration {
 public:
  Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
    max_seconds_ = max_seconds;
    max_ops_ = max_ops;
    ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
    ops_ = 0;
    start_at_ = Default_env->NowMicros();
  }

  int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

  bool Done(int64_t increment) {
    if (increment <= 0) increment = 1;  // avoid Done(0) and infinite loops
    ops_ += increment;

    if (max_seconds_) {
      // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
      auto granularity = Default_ops_between_duration_checks;
      if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
        uint64_t now = Default_env->NowMicros();
        return ((now - start_at_) / 1000000) >= max_seconds_;
      } else {
        return false;
      }
    } else {
      return ops_ > max_ops_;
    }
  }

 private:
  uint64_t max_seconds_;
  int64_t max_ops_;
  int64_t ops_per_stage_;
  int64_t ops_;
  uint64_t start_at_;
};

class Benchmark {
 private:
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<Cache> compressed_cache_;
  std::shared_ptr<const FilterPolicy> filter_policy_;
  const SliceTransform* prefix_extractor_;
  rocksdb::DBWithColumnFamilies db_;
  MockFileGenerator* mock_db_;
  bool mock_db_opened_ = false;
  int64_t num_;
  int key_size_;
  int value_size_;
  Options open_options_;

  WriteOptions write_options_;
  std::vector<std::string> keys_;

  class ErrorHandlerListener : public EventListener {
   public:
    ErrorHandlerListener()
        : mutex_(),
          cv_(&mutex_),
          no_auto_recovery_(false),
          recovery_complete_(false) {}

    ~ErrorHandlerListener() override {}

    void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/,
                              Status /*bg_error*/,
                              bool* auto_recovery) override {
      if (*auto_recovery && no_auto_recovery_) {
        *auto_recovery = false;
      }
    }

    void OnErrorRecoveryCompleted(Status /*old_bg_error*/) override {
      InstrumentedMutexLock l(&mutex_);
      recovery_complete_ = true;
      cv_.SignalAll();
    }

    bool WaitForRecovery(uint64_t abs_time_us) {
      InstrumentedMutexLock l(&mutex_);
      if (!recovery_complete_) {
        cv_.TimedWait(abs_time_us);
      }
      if (recovery_complete_) {
        recovery_complete_ = false;
        return true;
      }
      return false;
    }

    void EnableAutoRecovery(bool enable = true) { no_auto_recovery_ = !enable; }

   private:
    InstrumentedMutex mutex_;
    InstrumentedCondVar cv_;
    bool no_auto_recovery_;
    bool recovery_complete_;
  };

  std::shared_ptr<ErrorHandlerListener> listener_;

// Current the following isn't equivalent to OS_LINUX.
#if defined(__linux)
  static Slice TrimSpace(Slice s) {
    unsigned int start = 0;
    while (start < s.size() && isspace(s[start])) {
      start++;
    }
    unsigned int limit = static_cast<unsigned int>(s.size());
    while (limit > start && isspace(s[limit - 1])) {
      limit--;
    }
    return Slice(s.data() + start, limit - start);
  }
#endif

  std::shared_ptr<Cache> NewCache(int64_t capacity) {
    if (capacity <= 0) {
      return nullptr;
    }
    return NewLRUCache(static_cast<size_t>(capacity), 6,
                       false /*strict_capacity_limit*/, 0.0);
  }

 public:
  Benchmark(bool use_existing_db, std::string db, Options& options)
      : cache_(NewCache(8 << 20)),
        compressed_cache_(NewCache(-1)),
        filter_policy_(
            Default_bloom_bits >= 0
                ? NewBloomFilterPolicy(Default_bloom_bits,
                                       Default_use_block_based_filter)
                : nullptr),
        prefix_extractor_(NewFixedPrefixTransform(Default_prefix_size)),
        num_(0),
        key_size_(8),
        value_size_(10),
        open_options_(options) {
    // use simcache instead of cache
    if (Default_simcache_size >= 0) {
      if (Default_cache_numshardbits >= 1) {
        cache_ = NewSimCache(cache_, Default_simcache_size,
                             Default_cache_numshardbits);
      }
    }

    std::vector<std::string> files;

    if (!use_existing_db) {
      options.env = Default_env;
      if (!Default_wal_dir.empty()) {
        options.wal_dir = Default_wal_dir;
      }

      DestroyDB(db, options);
      if (!Default_wal_dir.empty()) {
        Default_env->DeleteDir(Default_wal_dir);
      }
    }
    Default_db = db;
    listener_.reset(new ErrorHandlerListener());
  }

  ~Benchmark() {
    db_.DeleteDBs();
    if (mock_db_ != nullptr && mock_db_opened_) {
      mock_db_->FreeDB();
    }
    //    delete mock_db_;

    delete prefix_extractor_;
    if (cache_.get() != nullptr) {
      // this will leak, but we're shutting down so nobody cares
      cache_->DisownData();
    }
  }

  Slice AllocateKey(std::unique_ptr<const char[]>* key_guard) {
    char* data = new char[key_size_];
    const char* const_data = data;
    key_guard->reset(const_data);
    return Slice(key_guard->get(), key_size_);
  }

  //  void Run(std::string command_list);
  void Run();

  std::unique_ptr<port::Thread> secondary_update_thread_;
  std::atomic<int> secondary_update_stopped_{0};
  uint64_t secondary_db_updates_ = 0;
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
    ChangePoint* point;
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    SetPerfLevel(static_cast<PerfLevel>(shared->perf_level));
    perf_context.EnablePerLevelPerfContext();
    thread->stats.Start(thread->tid);
    // detect whether change point worker
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  Stats RunBenchmark(int n, Slice name,
                     void (Benchmark::*method)(ThreadState*)) {
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;

    std::unique_ptr<ReporterAgent> reporter_agent;
    if (Default_report_interval_seconds > 0) {
      reporter_agent.reset(new ReporterAgent(Default_env, Default_report_file,
                                             Default_report_interval_seconds));
    }
    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
      arg[i].thread->stats.SetReporterAgent(reporter_agent.get());
      arg[i].thread->shared = &shared;

      Default_env->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    // Stats for some threads can be excluded.
    Stats merge_stats;
    for (int i = 0; i < n; i++) {
      merge_stats.Merge(arg[i].thread->stats);
    }

    merge_stats.Report(name);
    // here stops the system

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;

    return merge_stats;
  }

  void InitializeOptionsFromFlags(Options* opts);

  void InitializeOptionsGeneral(Options* opts) {
    Options& options = *opts;

    dbstats = ROCKSDB_NAMESPACE::CreateDBStatistics();
    dbstats->set_stats_level(static_cast<StatsLevel>(
        ROCKSDB_NAMESPACE::StatsLevel::kExceptDetailedTimers));
    //    options.create_missing_column_families = 1 > 1;
    // For XUAN to improve the performance
    options.min_write_buffer_number_to_merge = 1;
    //    options.max_write_buffer_number = 4;
    options.level0_file_num_compaction_trigger = 4;
    options.statistics = dbstats;
    options.wal_dir = Default_wal_dir;
    options.create_if_missing = !Default_use_existing_db;
    options.dump_malloc_stats = Default_dump_malloc_stats;
    options.stats_dump_period_sec =
        static_cast<unsigned int>(Default_stats_dump_period_sec);
    options.stats_persist_period_sec =
        static_cast<unsigned int>(Default_stats_persist_period_sec);
    options.persist_stats_to_disk = Default_persist_stats_to_disk;
    options.stats_history_buffer_size =
        static_cast<size_t>(Default_stats_history_buffer_size);

    options.compression_opts.level = Default_compression_level;
    options.compression_opts.max_dict_bytes =
        Default_compression_max_dict_bytes;
    options.compression_opts.zstd_max_train_bytes =
        Default_compression_zstd_max_train_bytes;
    options.compression_opts.parallel_threads =
        Default_compression_parallel_threads;
    // If this is a block based table, set some related options
    if (options.table_factory->Name() == BlockBasedTableFactory::kName &&
        options.table_factory->GetOptions() != nullptr) {
      BlockBasedTableOptions* table_options =
          reinterpret_cast<BlockBasedTableOptions*>(
              options.table_factory->GetOptions());
      if (Default_cache_size) {
        table_options->block_cache = cache_;
      }
      if (Default_bloom_bits >= 0) {
        table_options->filter_policy.reset(NewBloomFilterPolicy(
            Default_bloom_bits, Default_use_block_based_filter));
      }
    }
    if (Default_row_cache_size) {
      if (Default_cache_numshardbits >= 1) {
        options.row_cache =
            NewLRUCache(Default_row_cache_size, Default_cache_numshardbits);
      } else {
        options.row_cache = NewLRUCache(Default_row_cache_size);
      }
    }
    if (Default_enable_io_prio) {
      Default_env->LowerThreadPoolIOPriority(Env::LOW);
      Default_env->LowerThreadPoolIOPriority(Env::HIGH);
    }
    if (Default_enable_cpu_prio) {
      Default_env->LowerThreadPoolCPUPriority(Env::LOW);
      Default_env->LowerThreadPoolCPUPriority(Env::HIGH);
    }
    options.env = Default_env;
    options.listeners.emplace_back(listener_);
  }

  void Open(Options* opts, bool use_rocksdb, int bench_threads) {
    InitializeOptionsFromFlags(opts);
    InitializeOptionsGeneral(opts);
    if (use_rocksdb) {
      // open the rocksdb for a full time simulation
      OpenRocksDB(*opts, Default_db, &db_);
    } else {
      OpenMockDB(*opts, Default_db, bench_threads);
    }
  }

  void OpenRocksDB(Options& options, const std::string& db_name,
                   DBWithColumnFamilies* db) {
    Status s;
    // Open with column families if necessary.

    std::cout << "----------------Open RocksDB----------------" << std::endl;
    std::cout << db_name << std::endl;
    s = DB::Open(options, db_name, &db->db);

    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }
  void OpenMockDB(Options& options, const std::string& db_name,
                  int bench_threads) {
    Status s;
    // Open with column families if necessary.
    std::cout << "----------------Open MockDB----------------" << std::endl;

    std::cout << db_name << std::endl;

    mock_db_ =
        new MockFileGenerator(options.env, db_name, options, bench_threads);
    mock_db_->NewDB(!options.create_if_missing);
    mock_db_opened_ = true;
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  // benchmark implementation
  void Validate(ThreadState* thread);
  void Merge(ThreadState* thread);
  void Generate(ThreadState* thread);
  void Inject_LOAD(ThreadState* thread);
  void FillRandom(ThreadState* thread) { DoWrite(thread, RANDOM); }
  void FillSeq(ThreadState* thread) { DoWrite(thread, SEQUENTIAL); }
  void FillUnique(ThreadState* thread) { DoWrite(thread, UNIQUE_RANDOM); }
  void DoWrite(ThreadState* thread, WriteMode write_mode);
};

};  // namespace gear_db