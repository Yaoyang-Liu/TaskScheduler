#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <sys/types.h>

namespace ts {
struct JobSpec {
    std::string cmd; // 要执行的命令字符串
    int cpu_cores{1}; // 需要的cpu核心数
    size_t mem_mb{256}; // 需要的内存MB
    int timeout_sec{0}; // 超时秒数
    int priority{0}; // 优先级
};

enum class JobStatus {
    Pending, // 等待执行
    Running, // 正在执行
    Succeeded, // 成功结束
    Failed, // 失败结束
    Timeout, // 超时终止
    Cancelled // 被取消
};

struct Job {
    int id{0};
    JobSpec spec{};
    JobStatus status{JobStatus::Pending};
    pid_t pid{-1};
    pid_t pgid{-1};
    int exit_code{-1};
    std::chrono::steady_clock::time_point start_time{};
    std::chrono::steady_clock::time_point enqueue_time{};
    std::optional<std::chrono::steady_clock::time_point> end_time{};
    std::chrono::steady_clock::time_point sigterm_time{};
    bool sigkill_sent{false};
};
} // namespace ts