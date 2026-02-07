#pragma once

#include "job.h"
#include "resource_manager.h"
#include "metrics.h"
#include <vector>
#include <unordered_map>
#include <atomic>
#include <condition_variable>
#include <thread>
#include <queue>

namespace ts {

struct SchedulerOptions {
    ResourceQuota quota;
    int max_queue_size{1000};
    bool enable_priority{false};
    std::vector<std::string> cmd_whitelist;
    std::vector<std::string> cmd_blacklist;
};

struct JobComparator {
    bool operator()(const Job& a, const Job& b) const {
        if (a.spec.priority != b.spec.priority) {
            return a.spec.priority < b.spec.priority;
        }
        return a.enqueue_time > b.enqueue_time;
    }
};

class Scheduler {
public:
    explicit Scheduler(SchedulerOptions opts);
    ~Scheduler();

    // 提交任务，返回job id或-1
    int submit(const JobSpec& spec);

    // 启动后台线程
    void start();

    // 停止调度器等待后台退出
    void stop();

    bool idle() const;

    Metrics::Snapshot metrics() const;
private:
    bool pick_next_job(Job& out);
    bool check_whitelist(const JobSpec& spec);
    bool check_blacklist(const JobSpec& spec);
    bool launch_job(Job& job);
    void dispatcher_loop();
    void reaper_loop();
    SchedulerOptions opts_;
    ResourceManager rm_;
    std::priority_queue<Job, std::vector<Job>, JobComparator> pending_;
    std::unordered_map<int, Job> running_;
    mutable std::mutex mu_;
    int next_id_{1};
    std::atomic<bool> is_running_;
    std::thread dispatcher_thread_;
    std::thread reaper_thread_;
    std::condition_variable cv_;
    Metrics metrics_;
};

} // namespace ts
