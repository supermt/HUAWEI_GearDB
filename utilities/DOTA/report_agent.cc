//
// Created by jinghuan on 5/24/21.
//

#include "report_agent.h"

namespace ROCKSDB_NAMESPACE {
ReporterAgent::~ReporterAgent() {
  {
    std::unique_lock<std::mutex> lk(mutex_);
    stop_ = true;
    stop_cv_.notify_all();
  }
  reporting_thread_.join();
}
void ReporterAgent::InsertNewTuningPoints(ChangePoint point) {
  std::cout << "can't use change point @ " << point.change_timing
            << " Due to using default reporter" << std::endl;
};
void ReporterAgent::DetectAndTuning(int secs_elapsed) { secs_elapsed++; }

};  // namespace ROCKSDB_NAMESPACE
