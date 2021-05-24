//
// Created by jinghuan on 5/24/21.
//

#ifndef ROCKSDB_REPORTER_H
#define ROCKSDB_REPORTER_H
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
#include <string>
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
#include "utilities/blob_db/blob_db.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/bytesxor.h"
#include "utilities/merge_operators/sortlist.h"
#include "utilities/persistent_cache/block_cache_tier.h"

namespace ROCKSDB_NAMESPACE {

typedef std::vector<double> LSM_STATE;
class ReporterAgent;
struct ChangePoint;

class ReporterWithMoreDetails;
class ReporterAgentWithTuning;

struct ChangePoint {
  std::string opt;
  std::string value;
  int change_timing;
};
class ReporterAgent {
 private:
  std::string header_string_;

 public:
  static std::string Header() { return "secs_elapsed,interval_qps"; }
  ReporterAgent(Env* env, const std::string& fname,
                uint64_t report_interval_secs,
                std::string header_string = Header())
      : header_string_(header_string),
        env_(env),
        total_ops_done_(0),
        last_report_(0),
        report_interval_secs_(report_interval_secs),
        stop_(false) {
    auto s = env_->NewWritableFile(fname, &report_file_, EnvOptions());

    if (s.ok()) {
      s = report_file_->Append(header_string_ + "\n");
      //      std::cout << "opened report file" << std::endl;
    }
    if (s.ok()) {
      s = report_file_->Flush();
    }
    if (!s.ok()) {
      fprintf(stderr, "Can't open %s: %s\n", fname.c_str(),
              s.ToString().c_str());
      abort();
    }
    reporting_thread_ = port::Thread([&]() { SleepAndReport(); });
  }
  virtual ~ReporterAgent();

  // thread safe
  void ReportFinishedOps(int64_t num_ops) {
    total_ops_done_.fetch_add(num_ops);
  }

  virtual void InsertNewTuningPoints(ChangePoint point);

 protected:
  virtual void DetectAndTuning(int secs_elapsed);
  Env* env_;
  std::unique_ptr<WritableFile> report_file_;
  std::atomic<int64_t> total_ops_done_;
  int64_t last_report_;
  const uint64_t report_interval_secs_;
  ROCKSDB_NAMESPACE::port::Thread reporting_thread_;
  std::mutex mutex_;
  // will notify on stop
  std::condition_variable stop_cv_;
  bool stop_;
  void SleepAndReport() {
    auto time_started = env_->NowMicros();
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mutex_);
        if (stop_ ||
            stop_cv_.wait_for(lk, std::chrono::seconds(report_interval_secs_),
                              [&]() { return stop_; })) {
          // stopping
          break;
        }
        // else -> timeout, which means time for a report!
      }
      auto total_ops_done_snapshot = total_ops_done_.load();
      // round the seconds elapsed
      //      auto secs_elapsed = env_->NowMicros();
      auto secs_elapsed =
          (env_->NowMicros() - time_started + kMicrosInSecond / 2) /
          kMicrosInSecond;
      std::string report = ToString(secs_elapsed) + "," +
                           ToString(total_ops_done_snapshot - last_report_);

      auto s = report_file_->Append(report);
      this->DetectAndTuning(secs_elapsed);
      s = report_file_->Append("\n");
      if (s.ok()) {
        s = report_file_->Flush();
      }
      if (!s.ok()) {
        fprintf(stderr,
                "Can't write to report file (%s), stopping the reporting\n",
                s.ToString().c_str());
        break;
      }
      last_report_ = total_ops_done_snapshot;
    }
  }
};

class ReporterAgentWithTuning : public ReporterAgent {
 private:
  std::vector<ChangePoint> tuning_points;
  DBImpl* running_db_;
  std::string DOTAHeader() const {
    return "secs_elapsed,interval_qps,batch_size";
  }

 protected:
 public:
  ReporterAgentWithTuning(DBImpl* running_db, Env* env,
                          const std::string& fname,
                          uint64_t report_interval_secs)
      : ReporterAgent(env, fname, report_interval_secs, DOTAHeader()) {
    tuning_points = std::vector<ChangePoint>();
    tuning_points.clear();

    std::cout << "using reporter agent with change points." << std::endl;
    if (running_db == nullptr) {
      std::cout << "Missing parameter db_ to apply changes" << std::endl;
      abort();
    } else {
      running_db_ = running_db;
    }
  }

  void ApplyChangePoint(ChangePoint point) {
    //    //    db_.db->GetDBOptions();
    //    FLAGS_env->SleepForMicroseconds(point->change_timing * 1000000l);
    //    sleep(point->change_timing);
    std::unordered_map<std::string, std::string> new_options = {
        {point.opt, point.value}};
    Status s = running_db_->SetOptions(new_options);
    auto is_ok = s.ok() ? "suc" : "failed";
    std::cout << "Set " << point.opt + "=" + point.value + " " + is_ok
              << " after " << point.change_timing << " seconds running"
              << std::endl;
  }
  void InsertNewTuningPoints(ChangePoint point) {
    tuning_points.push_back(point);
  }
  const std::string memtable_size = "write_buffer_size";
  const std::string table_size = "target_file_size_base";
  const static unsigned long history_lsm_shape = 10;
  std::deque<LSM_STATE> shape_list;
  const size_t default_memtable_size = 64 << 20;

  const float threashold = 0.5;

  bool reach_lsm_double_line(size_t sec_elpased, Options* opt) {
    int counter[history_lsm_shape] = {0};
    for (LSM_STATE shape : shape_list) {
      int max_level = 0;
      int max_score = -1;
      unsigned long len = shape.size();
      for (unsigned long i = 0; i < len; i++) {
        if (shape[i] > max_score) {
          max_score = shape[i];
          max_level = i;
          if (shape[i] == 0) break;  // there is no file in this level
        }
      }
      //      std::cout << "max level is " << max_level << std::endl;
      counter[max_level]++;
    }
    ChangePoint point;
    for (unsigned long i = 0; i < history_lsm_shape; i++) {
      if (counter[i] > threashold * history_lsm_shape) {
        //        std::cout << "Apply changes due to crowded level  " << i <<
        //        std::endl;
        // for in each detect window, it suppose to happen a lot of single level
        // compaction, but we need to start with level 2, since level 1 is much
        // to common
        if (i <= 1) {
          if (opt->write_buffer_size > 4 * default_memtable_size) {
            point.change_timing = sec_elpased + history_lsm_shape / 10;
            point.opt = memtable_size;
            point.value = ToString(default_memtable_size);
            tuning_points.push_back(point);
            point.opt = table_size;
            tuning_points.push_back(point);
            report_file_->Append("," + point.opt + "," + point.value);
          }
        } else if (opt->write_buffer_size <= default_memtable_size * 8) {
          point.change_timing = sec_elpased + history_lsm_shape / 10;
          point.opt = memtable_size;
          point.value = ToString(opt->write_buffer_size * 2);
          tuning_points.push_back(point);
          point.opt = table_size;
          tuning_points.push_back(point);
          report_file_->Append("," + point.opt + "," + point.value);
        }

        return true;
      }
    }
    return false;
  }

  void Detect(int sec_elpased) {
    //    auto qps = total_ops_done_.load() - last_report_;
    //    int db_size = 64;
    //    auto slow_down_trigger = opt.level0_slowdown_writes_trigger;
    DBImpl* dbfull = running_db_;
    VersionSet* test = dbfull->GetVersionSet();

    Options opt = running_db_->GetOptions();
    auto cfd = test->GetColumnFamilySet()->GetDefault();
    auto vstorage = cfd->current()->storage_info();
    LSM_STATE temp;
    for (int i = 0; i < vstorage->num_levels(); i++) {
      double score = vstorage->CompactionScore(i);
      temp.push_back(score);
    }
    if (shape_list.size() > history_lsm_shape) {
      shape_list.pop_front();
    }
    shape_list.push_back(temp);
    //    std::cout << "shape list pushed in with length " << temp.size()
    //              << std::endl;
    reach_lsm_double_line(sec_elpased, &opt);
    //    LSM_STATE temp;
    //
    //    for (int level = 0; level < vstorage->num_levels(); ++level) {
    //      temp.push_back(vstorage->NumLevelFiles(level));
    //    }
    //
    //    if (shape_list.size() > history_lsm_shape) {
    //      shape_list.pop_front();
    //    }
    //    shape_list.push_back(temp);
    //    if (reach_lsm_double_line()) {
    //    }

    ChangePoint testpoint;

    //    uint64_t size = version;
  }

  void PopChangePoints(int secs_elapsed) {
    for (auto it = tuning_points.begin(); it != tuning_points.end(); it++) {
      if (it->change_timing <= secs_elapsed) {
        if (running_db_ != nullptr) {
          ApplyChangePoint(*it);
        }
        tuning_points.erase(it--);
      }
    }
  }

  void DetectAndTuning(int secs_elapsed) {
    Detect(secs_elapsed);
    if (tuning_points.empty()) {
      return;
    }
    PopChangePoints(secs_elapsed);
  }
};  // end ReporterWithTuning
class ReporterWithMoreDetails : public ReporterAgent {
 private:
  DBImpl* db_ptr;
  std::string detailed_header() {
    return ReporterAgent::Header() + ",num_compaction,num_flushes,lsm_shape";
  }

 public:
  ReporterWithMoreDetails(DBImpl* running_db, Env* env,
                          const std::string& fname,
                          uint64_t report_interval_secs)
      : ReporterAgent(env, fname, report_interval_secs, detailed_header()) {
    if (running_db == nullptr) {
      std::cout << "Missing parameter db_ to record more details" << std::endl;
      abort();

    } else {
      db_ptr = reinterpret_cast<DBImpl*>(running_db);
    }
  }

  void RecordCompactionQueue() {
    //    auto compaction_queue_ptr = db_ptr->getCompactionQueue();
    int num_running_compactions = db_ptr->num_running_compactions();
    int num_running_flushes = db_ptr->num_running_flushes();

    //    std::stringstream compaction_stat_list_ss;
    //    std::string compaction_stat;
    //    compaction_stat_list_ss << "[";
    //    for (ColumnFamilyData* cfd : *compaction_queue_ptr) {
    //      cfd->internal_stats();
    //    }
    report_file_->Append(ToString(num_running_compactions) + ",");
    report_file_->Append(ToString(num_running_flushes) + ",");
    //    compaction_stat_list_ss >> compaction_stat;
    //    compaction_stat.replace(compaction_stat.end() - 1,
    //    compaction_stat.end(),
    //                            "]");
    //    report_file_->Append(compaction_stat);
  }
  void RecordLSMShape() {
    auto vstorage = db_ptr->GetVersionSet()
                        ->GetColumnFamilySet()
                        ->GetDefault()
                        ->current()
                        ->storage_info();

    report_file_->Append("[");
    int i = 0;
    for (i = 0; i < vstorage->num_levels() - 1; i++) {
      int file_count = vstorage->NumLevelFiles(i);
      report_file_->Append(ToString(file_count) + ",");
    }
    report_file_->Append(ToString(vstorage->NumLevelFiles(i)) + "]");
  }

  void DetectAndTuning(int secs_elapsed) {
    report_file_->Append(",");
    RecordCompactionQueue();
    RecordLSMShape();
    secs_elapsed++;
  }
};

};  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_REPORTER_H
