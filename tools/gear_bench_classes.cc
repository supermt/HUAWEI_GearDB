//
// Created by jinghuan on 7/12/21.
//
#include "rocksdb/gear_bench_classes.h"

namespace ROCKSDB_NAMESPACE {
KeyGenerator::KeyGenerator(Random64* rand, WriteMode mode, uint64_t num,
                           uint64_t seed, int key_size, uint64_t min_value)
    : rand_(rand), mode_(mode), distinct_num_(num), next_(min_value) {
  if (mode_ == UNIQUE_RANDOM) {
    values_.resize(distinct_num_);
    for (uint64_t i = min_value; i < distinct_num_; ++i) {
      values_[i] = i;
    }
    RandomShuffle(values_.begin(), values_.end(), static_cast<uint32_t>(seed));
  }
  key_size_ = key_size;
  key_slice_ = AllocateKey(&key_guard, key_size_);
}

// Static Functions
std::string KeyGenerator::GenerateKeyFromInt(uint64_t v, uint64_t num_keys,
                                             Slice* key, int key_size) {
  Slice key_results;
  std::unique_ptr<const char[]> temp_buf;
  AllocateKey(&temp_buf, key_size);
  if (key == nullptr) {
    key = &key_results;
  }
  v = v % num_keys;
  char* start = const_cast<char*>(key->data());
  char* pos = start;
  int bytes_to_fill = std::min(key_size - static_cast<int>(pos - start), 8);
  if (port::kLittleEndian) {
    for (int i = 0; i < bytes_to_fill; ++i) {
      pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
    }
  } else {
    memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
  }
  pos += bytes_to_fill;
  if (key_size > pos - start) {
    memset(pos, '0', key_size - (pos - start));
  }
  return key->ToString();
}
Slice KeyGenerator::AllocateKey(std::unique_ptr<const char[]>* key_guard,
                                int key_size) {
  char* data = new char[key_size];
  const char* const_data = data;
  key_guard->reset(const_data);
  return Slice(key_guard->get(), key_size);
}
KeyGenerator::~KeyGenerator() = default;

void MockFileGenerator::ReOpenDB() {
  DBImpl* impl = new DBImpl(DBOptions(options_), dbname_);
  std::vector<ColumnFamilyDescriptor> column_families;
  cf_options_.table_factory = gear_table_factory;
  cf_options_.merge_operator = merge_op_;
  cf_options_.compaction_filter = compaction_filter_.get();
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options_);
  versions_.reset(new VersionSet(dbname_, &db_options_, env_options_,
                                 table_cache_.get(), &write_buffer_manager_,
                                 &write_controller_,
                                 /*block_cache_tracer=*/nullptr));
  compaction_job_stats_.Reset();
  SetIdentityFile(env_, dbname_);
  Status s = versions_->Recover(column_families, false);
  if (!s.ok()) exit(-2);
  env_->SleepForMicroseconds(kMicrosInSecond);
  cfd_ = versions_->GetColumnFamilySet()->GetDefault();
  ColumnFamilyHandle* handle =
      new ColumnFamilyHandleImpl(cfd_, impl, impl->mutex());

  for (auto file : versions_->GetColumnFamilySet()
                       ->GetDefault()
                       ->current()
                       ->storage_info()
                       ->LevelFiles(options_.num_levels - 1)) {
    file->l2_position = VersionStorageInfo::l2_large_tree_index;
  }
  for (int i = 0; i < bench_threads_; i++) {
    writers_[i].reset(new SstFileWriter(env_options_, options_, handle));
  }
}

void MockFileGenerator::NewDB(bool use_existing_data) {
  DBImpl* impl = new DBImpl(DBOptions(options_), dbname_);
  if (!use_existing_data) {
    DestroyDB(dbname_, Options());
  }
  //  DestroyDB(dbname_, Options());
  versions_.reset(new VersionSet(dbname_, &db_options_, env_options_,
                                 table_cache_.get(), &write_buffer_manager_,
                                 &write_controller_,
                                 /*block_cache_tracer=*/nullptr));
  compaction_job_stats_.Reset();
  SetIdentityFile(env_, dbname_);

  VersionEdit new_db;
  if (db_options_.write_dbid_to_manifest) {
    std::string db_id;
    impl->GetDbIdentityFromIdentityFile(&db_id);
    new_db.SetDBId(db_id);
  }
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  std::unique_ptr<WritableFile> file;
  Status s = env_->NewWritableFile(
      manifest, &file, env_->OptimizeForManifestWrite(env_options_));
  assert(s.ok());
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      NewLegacyWritableFileWrapper(std::move(file)), manifest, env_options_));
  {
    log::Writer log(std::move(file_writer), 0, false);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
  }
  // Make "CURRENT" file that points to the new manifest file.
  s = SetCurrentFile(fs_.get(), dbname_, 1, nullptr);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
  }
  std::vector<ColumnFamilyDescriptor> column_families;
  cf_options_.table_factory = gear_table_factory;
  cf_options_.merge_operator = merge_op_;
  cf_options_.compaction_filter = compaction_filter_.get();
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options_);
  s = versions_->Recover(column_families, false);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
  }
  cfd_ = versions_->GetColumnFamilySet()->GetDefault();
  ColumnFamilyHandle* handle =
      new ColumnFamilyHandleImpl(cfd_, impl, impl->mutex());

  for (int i = 0; i < bench_threads_; i++) {
    writers_[i].reset(new SstFileWriter(env_options_, options_, handle));
  }

}

Status MockFileGenerator::AddMockFile(const stl_wrappers::KVMap& contents,
                                      int level, int l2_position,
                                      int thread_id) {
  bool first_key = true;
  std::string smallest, largest;
  InternalKey smallest_key, largest_key;
  SequenceNumber smallest_seqno = kMaxSequenceNumber;
  SequenceNumber largest_seqno = 0;

  mutex_.Lock();
  uint64_t file_number = versions_->NewFileNumber();
  mutex_.Unlock();

  Status s;
  auto sst_name = GenerateFileName(file_number);
  s = writers_[thread_id]->Open(sst_name);
  if (!s.ok()) return s;
  for (auto kv : contents) {
    ParsedInternalKey key;
    std::string skey;
    std::string value;
    std::tie(skey, value) = kv;
    ParseInternalKey(skey, &key);

    smallest_seqno = std::min(smallest_seqno, key.sequence);
    largest_seqno = std::max(largest_seqno, key.sequence);
    if (first_key ||
        cfd_->user_comparator()->Compare(key.user_key, smallest) < 0) {
      smallest.assign(key.user_key.data(), key.user_key.size());
      smallest_key.DecodeFrom(skey);
    }
    if (first_key ||
        cfd_->user_comparator()->Compare(key.user_key, largest) > 0) {
      largest.assign(key.user_key.data(), key.user_key.size());
      largest_key.DecodeFrom(skey);
    }
    first_key = false;
    writers_[thread_id]->Put(key.user_key, value);
  }
  s = writers_[thread_id]->Finish();
  //  int size = writer_->FileSize();
  if (!s.ok()) {
    return s;
  }
  VersionEdit edit;
  uint64_t oldest_blob_file_number = kInvalidBlobFileNumber;

  edit.AddFile(level, file_number, 0, writers_[thread_id]->FileSize(),
               smallest_key, largest_key, smallest_seqno, largest_seqno, false,
               oldest_blob_file_number, kUnknownOldestAncesterTime,
               kUnknownOldestAncesterTime, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName, l2_position);
  mutex_.Lock();
  s = versions_->LogAndApply(versions_->GetColumnFamilySet()->GetDefault(),
                             mutable_cf_options_, &edit, &mutex_);
  mutex_.Unlock();
  //  assert(s.ok());
  return s;
}

Status MockFileGenerator::AddMockFile(uint64_t start_num, uint64_t end_num,
                                      KeyGenerator* key_gen,
                                      SequenceNumber sequence_number,
                                      int thread_id, int level,
                                      int l2_position) {
  InternalKey smallest_key, largest_key;
  SequenceNumber smallest_seqno = sequence_number;
  SequenceNumber largest_seqno = sequence_number;
  uint64_t start = env_->NowMicros();
  uint64_t file_number = versions_->NewFileNumber();
  Status s;
  auto sst_name = GenerateFileName(file_number);
  s = writers_[thread_id]->Open(sst_name);
  if (!s.ok()) return s;

  std::string value = "0000000000";
  uint64_t i = 0;
  for (i = start_num; i < end_num; i++) {
    std::string skey = key_gen->GenerateKeyFromInt(i);
    s = writers_[thread_id]->Put(skey, value);
    assert(s.ok());
  }
  std::string temp = key_gen->GenerateKeyFromInt(start_num);
  smallest_key = InternalKey(temp, sequence_number, kTypeValue);
  temp = key_gen->GenerateKeyFromInt(end_num);
  largest_key = InternalKey(temp, sequence_number, kTypeValue);

  s = writers_[thread_id]->Finish();
  //  int size = writer_->FileSize();
  if (!s.ok()) {
    return s;
  }
  VersionEdit edit;
  uint64_t oldest_blob_file_number = kInvalidBlobFileNumber;

  edit.AddFile(level, file_number, 0, writers_[thread_id]->FileSize(),
               smallest_key, largest_key, smallest_seqno, largest_seqno, false,
               oldest_blob_file_number, kUnknownOldestAncesterTime,
               kUnknownOldestAncesterTime, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName, l2_position);
  mutex_.Lock();
  s = versions_->LogAndApply(versions_->GetColumnFamilySet()->GetDefault(),
                             mutable_cf_options_, &edit, &mutex_);
  mutex_.Unlock();
  uint64_t total = env_->NowMicros() - start;
  total /= kMicrosInSecond;
  std::cout << "Single File cost(sec): " << total << std::endl;
  //  assert(s.ok());
  return s;
}

Status MockFileGenerator::TriggerCompaction() {
  cfd_ = versions_->GetColumnFamilySet()->GetDefault();
  Status s;
  // Trigger a compaction manually
  //  std::vector<CompactionInputFiles> input_files;
  CompactionInputFiles l2_input_files;
  l2_input_files.level = 2;
  for (auto file : cfd_->current()->storage_info()->LevelFiles(2)) {
    l2_input_files.files.push_back(file);
  }
  GearCompactionPicker gear_picker(*cfd_->ioptions(), &icmp_);
  LogBuffer log_buffer(InfoLogLevel::DEBUG_LEVEL, db_options_.info_log.get());
  //  Compaction* compaction_ptr =
  //      cfd_->PickCompaction(this->mutable_cf_options_, &log_buffer);
  auto start_micro = env_->NowMicros();
  std::cout << "Start picking compaction files at: " << start_micro
            << std::endl;
  Compaction* compaction_ptr = gear_picker.PickCompaction(
      cfd_->GetName(), *cfd_->GetLatestMutableCFOptions(),
      cfd_->current()->storage_info(), &log_buffer);
  assert(compaction_ptr != nullptr);
  std::cout << "Pick compaction complete at: " << env_->NowMicros()
            << std::endl;

  if (compaction_ptr == nullptr) {
    std::cout << "No need for a compaction run, trigger a compaction manually "
              << std::endl;
    Compaction compaction(cfd_->current()->storage_info(), *cfd_->ioptions(),
                          *cfd_->GetLatestMutableCFOptions(), {l2_input_files},
                          2, options_.write_buffer_size * 100,
                          options_.max_compaction_bytes, 0, kNoCompression,
                          cfd_->GetLatestMutableCFOptions()->compression_opts,
                          options_.max_background_jobs, {}, true);
    compaction_ptr = &compaction;
  }
  //    else {
  //
  //  }

  compaction_ptr->SetInputVersion(cfd_->current());
  for (auto level : *compaction_ptr->inputs()) {
    if (level.files.size() != 0)
      std::cout << "Collected files in level " << level.level << " : "
                << level.files.size() << std::endl;
  }
  std::vector<SequenceNumber> snapshots = {};
  SequenceNumber earliest_write_conflict_snapshot = kMaxSequenceNumber;
  mutex_.Lock();
  EventLogger event_logger(db_options_.info_log.get());
  SnapshotChecker* snapshot_checker = nullptr;
  std::cout << "Start the compaction job at: " << env_->NowMicros()
            << std::endl;
  CompactionJob compaction_job(
      0, compaction_ptr, db_options_, env_options_, versions_.get(),
      &shutting_down_, preserve_deletes_seqnum_, &log_buffer, nullptr, nullptr,
      nullptr, &mutex_, &error_handler_, snapshots,
      earliest_write_conflict_snapshot, snapshot_checker, table_cache_,
      &event_logger, false, false, dbname_, &compaction_job_stats_,
      Env::Priority::LOW);

  compaction_job.Prepare();
  std::cout << "Compaction job prepared at: " << env_->NowMicros() << std::endl;
  mutex_.Unlock();
  start_micro = env_->NowMicros();
  s = compaction_job.Run();
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return s;
  }
  mutex_.Lock();
  s = compaction_job.Install(*cfd_->GetLatestMutableCFOptions());
  uint64_t end = env_->NowMicros();
  std::cout << "Compaction Finished at :" << end << std::endl;
  std::cout << "time cost of Compaction" << (end - start_micro) / 1000000
            << "secs" << std::endl;
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
  }
  mutex_.Unlock();
  return s;
}
Status MockFileGenerator::CreateFileByKeyRange(uint64_t smallest_key,
                                               uint64_t largest_key,
                                               KeyGenerator* key_gen,
                                               int thread_id,
                                               SequenceNumber start_seq) {
  Status s;
  SequenceNumber sequence_number = start_seq;
  s = AddMockFile(smallest_key, largest_key, key_gen, sequence_number,
                  thread_id, 2, VersionStorageInfo::l2_large_tree_index);

  sequence_number++;
  SetLastSequence(sequence_number);
  return s;
}
void MockFileGenerator::FreeDB() {
  for (int i = 0; i < bench_threads_; i++) {
    writers_[i].release();
  }
  versions_.release();
}
uint64_t SeqKeyGenerator::Next() {
  next_++;
  return std::max(min_ + next_, std::numeric_limits<uint64_t>::max());
}
}  // namespace ROCKSDB_NAMESPACE