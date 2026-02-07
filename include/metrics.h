#pragma once

#include <atomic>
#include <string>

namespace ts {
class Metrics {
public:
    struct Snapshot {
        int64_t submitted{0};
        int64_t rejected{0};
        int64_t running{0};
        int64_t succeeded{0};
        int64_t failed{0};
        int64_t timeout{0};
        double queue_wait_ms_avg{0.0};
        int64_t queue_wait_ms_max{0};
        int64_t queue_wait_count{0};
        int pending{0};
    };
    
    void inc_submitted() { submitted_.fetch_add(1, std::memory_order_relaxed); }
    void inc_rejected() { rejected_.fetch_add(1, std::memory_order_relaxed); }
    void inc_running() { running_.fetch_add(1, std::memory_order_relaxed); }
    void dec_running() { running_.fetch_sub(1, std::memory_order_relaxed); }
    void inc_succeeded() { succeeded_.fetch_add(1, std::memory_order_relaxed); }
    void inc_failed() { failed_.fetch_add(1, std::memory_order_relaxed); }
    void inc_timeout() { timeout_.fetch_add(1, std::memory_order_relaxed); }
    void inc_launch_failed() { launch_failed_.fetch_add(1, std::memory_order_relaxed); }

    void add_queue_wait(int64_t wait_ms);

    Snapshot snapshot(int pending) const {
        Snapshot s;
        s.submitted = submitted_.load(std::memory_order_relaxed);
        s.rejected = rejected_.load(std::memory_order_relaxed);
        s.running = running_.load(std::memory_order_relaxed);
        s.succeeded = succeeded_.load(std::memory_order_relaxed);
        s.failed = failed_.load(std::memory_order_relaxed);
        s.timeout = timeout_.load(std::memory_order_relaxed);
        auto total_wait = queue_wait_ms_total_.load(std::memory_order_relaxed);
        auto count = queue_wait_count_.load(std::memory_order_relaxed);
        s.queue_wait_count = count;
        s.queue_wait_ms_max = queue_wait_ms_max_.load(std::memory_order_relaxed);
        s.queue_wait_ms_avg = count > 0 ? static_cast<double>(total_wait) / static_cast<double>(count) : 0.0;
        s.pending = pending;
        return s;
    }

    std::string to_prometheus(const Snapshot& s) const;

private:
    std::atomic<int64_t> submitted_{0};
    std::atomic<int64_t> rejected_{0};
    std::atomic<int64_t> running_{0};
    std::atomic<int64_t> succeeded_{0};
    std::atomic<int64_t> failed_{0};
    std::atomic<int64_t> timeout_{0};
    std::atomic<int64_t> launch_failed_{0};
    std::atomic<int64_t> queue_wait_ms_total_{0};
    std::atomic<int64_t> queue_wait_count_{0};
    std::atomic<int64_t> queue_wait_ms_max_{0};
};
}