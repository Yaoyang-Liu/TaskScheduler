#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <sys/types.h>

namespace ts {
class JobSepc {
    std::string cmd; // 要执行的命令字符串
    int cpu_cores{1}; // 需要的cpu核心数
    size_t mem_mb{256}; // 需要的内存MB
    int timeout_ms{0}; // 超时秒数
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
    JobSepc sepc{};
    JobStatus status{JobStatus::Pending};
    pid_t pid{-1};
    pid_t pgid{-1};
    int exit_code{-1};
};
} // namespace ts