#include "metrics.h"

namespace ts {
    void Metrics::add_queue_wait(int64_t wait_ms) {
        queue_wait_ms_total_.fetch_add(wait_ms, std::memory_order_relaxed);
        queue_wait_count_.fetch_add(1, std::memory_order_relaxed);
        int64_t prev = queue_wait_ms_max_.load(std::memory_order_relaxed);
        while (wait_ms > prev && !queue_wait_ms_max_.compare_exchange_weak(prev, wait_ms, std::memory_order_relaxed)) {

        }
    }
}