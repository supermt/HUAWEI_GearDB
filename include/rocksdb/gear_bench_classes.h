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
#include "rocksdb/write_buffer_manager.h"
#include "sst_file_reader.h"

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

  uint64_t Next() {
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
    return std::numeric_limits<uint64_t>::max();
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

 private:
  Random64* rand_;
  WriteMode mode_;
  uint64_t min_;
  const uint64_t distinct_num_;
  uint64_t next_;
  int key_size_ = 15;
  std::vector<uint64_t> values_;
  Slice key_slice_;
  std::unique_ptr<const char[]> key_guard;
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
    assert(reader.Open(file_name).ok());
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
class CountingLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* /*format*/, va_list /*ap*/) override { log_count++; }
  size_t log_count;
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
  std::unique_ptr<SstFileWriter> writer_;
  ErrorHandler error_handler_;
  // function field.
  MockFileGenerator(Env* env, const std::string& db_name, Options& opt)
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
        error_handler_(nullptr, db_options_, &mutex_) {
    env_->CreateDirIfMissing(db_name);
    env_->CreateDir(db_name + opt.index_dir_prefix);
    db_options_.env = env_;
    db_options_.fs = fs_;

    GearTableOptions gearTableOptions;
    gearTableOptions.encoding_type = kPlain;
    gearTableOptions.user_key_len = 15;
    gearTableOptions.user_value_len = 10;
    gear_table_factory =
        std::shared_ptr<TableFactory>(NewGearTableFactory(gearTableOptions));

    assert(!db_options_.db_paths.empty());
  }
  std::string GenerateFileName(uint64_t file_number) {
    FileMetaData meta;
    std::vector<DbPath> db_paths;
    db_paths.emplace_back(dbname_, std::numeric_limits<uint64_t>::max());
    meta.fd = FileDescriptor(file_number, 0, 0);
    return TableFileName(db_paths, meta.fd.GetNumber(), meta.fd.GetPathId());
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
  Status AddMockFile(const stl_wrappers::KVMap& contents, int level = 2,
                     int l2_position = VersionStorageInfo::l2_large_tree_index);
  Status TriggerCompaction(bool* triggered);
  void NewDB(bool use_existing_data);
  void FreeDB();
  Status CreateFileByKeyRange(uint64_t smallest_key, uint64_t largest_key,
                              KeyGenerator* key_gen,
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
      content.emplace(ikey.Encode().ToString(), value);
    }
    return content;
  }
};
}  // namespace ROCKSDB_NAMESPACE
