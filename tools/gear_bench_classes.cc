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
void CompactionPickerDummy::Add(int level, uint32_t file_number,
                                const char* smallest, const char* largest,
                                int l2_position, uint64_t file_size,
                                uint32_t path_id, SequenceNumber smallest_seq,
                                SequenceNumber largest_seq,
                                size_t compensated_file_size,
                                bool marked_for_compact) {
  VersionStorageInfo* vstorage;
  if (temp_vstorage_) {
    vstorage = temp_vstorage_.get();
  } else {
    vstorage = vstorage_.get();
  }
  assert(level < vstorage->num_levels());
  FileMetaData* f = new FileMetaData(
      file_number, path_id, file_size,
      InternalKey(smallest, smallest_seq, kTypeValue),
      InternalKey(largest, largest_seq, kTypeValue), smallest_seq, largest_seq,
      marked_for_compact, kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
      kUnknownFileCreationTime, kUnknownFileChecksum,
      kUnknownFileChecksumFuncName);
  f->compensated_file_size =
      (compensated_file_size != 0) ? compensated_file_size : file_size;
  f->l2_position = l2_position;
  vstorage->AddFile(level, f);
  files_.emplace_back(f);
  file_map_.insert({file_number, {f, level}});
}
void CompactionPickerDummy::UpdateVersionStorageInfo() {
  if (temp_vstorage_) {
    VersionBuilder builder(FileOptions(), &ioptions_, nullptr, vstorage_.get(),
                           nullptr);
    builder.SaveTo(temp_vstorage_.get());
    vstorage_ = std::move(temp_vstorage_);
  }
  vstorage_->CalculateBaseBytes(ioptions_, mutable_cf_options_);
  vstorage_->UpdateFilesByCompactionPri(ioptions_.compaction_pri);
  vstorage_->UpdateNumNonEmptyLevels();
  vstorage_->GenerateFileIndexer();
  vstorage_->GenerateLevelFilesBrief();
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  vstorage_->GenerateLevel0NonOverlapping();
  vstorage_->ComputeFilesMarkedForCompaction();
  vstorage_->SetFinalized();
}
void CompactionPickerDummy::AddFileToVersionStorage(
    int level, uint32_t file_number, const char* smallest, const char* largest,
    uint64_t file_size, uint32_t path_id, SequenceNumber smallest_seq,
    SequenceNumber largest_seq, size_t compensated_file_size,
    bool marked_for_compact) {
  VersionStorageInfo* base_vstorage = vstorage_.release();
  vstorage_.reset(new VersionStorageInfo(&icmp_, ucmp_, options_.num_levels,
                                         kCompactionStyleUniversal,
                                         base_vstorage, false));
  Add(level, file_number, smallest, largest, file_size, path_id, smallest_seq,
      largest_seq, compensated_file_size, marked_for_compact);

  VersionBuilder builder(FileOptions(), &ioptions_, nullptr, base_vstorage,
                         nullptr);
  builder.SaveTo(vstorage_.get());
  UpdateVersionStorageInfo();
}

void MockFileGenerator::ReOpenDB() {
  DBImpl* impl = new DBImpl(DBOptions(options_), dbname_);
  std::vector<ColumnFamilyDescriptor> column_families;
  cf_options_.table_factory = gear_table_factory;
  cf_options_.merge_operator = merge_op_;
  cf_options_.compaction_filter = compaction_filter_.get();
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options_);
  versions_.release();
  versions_.reset(new VersionSet(dbname_, &db_options_, env_options_,
                                 table_cache_.get(), &write_buffer_manager_,
                                 &write_controller_,
                                 /*block_cache_tracer=*/nullptr));
  compaction_job_stats_.Reset();
  SetIdentityFile(env_, dbname_);
  assert(versions_->Recover(column_families, false).ok());
  cfd_ = versions_->GetColumnFamilySet()->GetDefault();
  ColumnFamilyHandle* handle =
      new ColumnFamilyHandleImpl(cfd_, impl, impl->mutex());
  writer_.reset(new SstFileWriter(env_options_, options_, handle));
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
  assert(s.ok());
  std::vector<ColumnFamilyDescriptor> column_families;
  cf_options_.table_factory = gear_table_factory;
  cf_options_.merge_operator = merge_op_;
  cf_options_.compaction_filter = compaction_filter_.get();
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options_);
  assert(versions_->Recover(column_families, false).ok());
  cfd_ = versions_->GetColumnFamilySet()->GetDefault();
  ColumnFamilyHandle* handle =
      new ColumnFamilyHandleImpl(cfd_, impl, impl->mutex());
  writer_.reset(new SstFileWriter(env_options_, options_, handle));
}

Status MockFileGenerator::AddMockFile(const stl_wrappers::KVMap& contents,
                                      int level) {
  assert(this->cf_options_.compaction_style == kCompactionStyleGear);
  assert(level = 2);

  bool first_key = true;
  std::string smallest, largest;
  InternalKey smallest_key, largest_key;
  SequenceNumber smallest_seqno = kMaxSequenceNumber;
  SequenceNumber largest_seqno = 0;

  uint64_t file_number = versions_->NewFileNumber();
  Status s;
  writer_->Open(GenerateFileName(file_number));

  for (auto kv : contents) {
    ParsedInternalKey key;
    std::string skey;
    std::string value;
    std::tie(skey, value) = kv;
    bool parsed = ParseInternalKey(skey, &key);

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
    writer_->Put(key.user_key, value);
  }
  s = writer_->Finish();
  int size = writer_->FileSize();
  if (!s.ok()) {
    return s;
  }
  VersionEdit edit;
  uint64_t oldest_blob_file_number = kInvalidBlobFileNumber;

  edit.AddFile(level, file_number, 0, writer_->FileSize(), smallest_key,
               largest_key, smallest_seqno, largest_seqno, false,
               oldest_blob_file_number, kUnknownOldestAncesterTime,
               kUnknownOldestAncesterTime, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName);
  mutex_.Lock();
  s = versions_->LogAndApply(versions_->GetColumnFamilySet()->GetDefault(),
                             mutable_cf_options_, &edit, &mutex_);
  mutex_.Unlock();
  //  assert(s.ok());
  return s;
}

void MockFileGenerator::TriggerCompaction(CompactionPickerDummy* picker) {}
Status MockFileGenerator::CreateFileByKeyRange(uint64_t smallest_key,
                                               uint64_t largest_key,
                                               KeyGenerator* key_gen) {
  Status s;
  SequenceNumber sequence_number = 0;
  stl_wrappers::KVMap content;
  assert(content.empty());
  std::string value = "1234567890";
  for (uint64_t i = smallest_key; i < largest_key; i++) {
    auto key = key_gen->GenerateKeyFromInt(i);
    InternalKey ikey(key, ++sequence_number, kTypeValue);
    content.emplace(ikey.Encode().ToString(), value);
  }
  s = AddMockFile(content, 2);
  SetLastSequence(sequence_number);
  return s;
}
void MockFileGenerator::FreeDB() {
  writer_.release();
  versions_.release();
}
}  // namespace ROCKSDB_NAMESPACE