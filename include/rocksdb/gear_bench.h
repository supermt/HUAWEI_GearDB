//
// Created by jinghuan on 7/6/21.
//
#pragma once

#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/rocksdb_namespace.h"
namespace ROCKSDB_NAMESPACE {
static std::string ColumnFamilyName(size_t i) {
  if (i == 0) {
    return ROCKSDB_NAMESPACE::kDefaultColumnFamilyName;
  } else {
    char name[100];
    snprintf(name, sizeof(name), "column_family_name_%06zu", i);
    return std::string(name);
  }
}

enum WriteMode { RANDOM, SEQUENTIAL, UNIQUE_RANDOM };

class KeyGenerator {
 public:
  KeyGenerator(Random64* rand, WriteMode mode, uint64_t num,
               uint64_t /*num_per_set*/ = 64 * 1024);

  uint64_t Next() {
    switch (mode_) {
      case SEQUENTIAL:
        return next_++;
      case RANDOM:
        return rand_->Next() % num_;
      case UNIQUE_RANDOM:
        assert(next_ < num_);
        return values_[next_++];
    }
    assert(false);
    return std::numeric_limits<uint64_t>::max();
  }

 private:
  Random64* rand_;
  WriteMode mode_;
  const uint64_t num_;
  uint64_t next_;
  std::vector<uint64_t> values_;
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

int gear_bench(int argc, char** argv);
}  // namespace ROCKSDB_NAMESPACE