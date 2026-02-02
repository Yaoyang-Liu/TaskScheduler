#pragma once

#include <chrono>
#include <string>
#include <iostream>
#include <mutex>

namespace ts {

class Logger {
public:
    enum class Level { INFO, WARN, ERROR };

    // 设置全局日志级别（默认 INFO）
    static void set_level(Level level);

    // 获取单例实例
    static Logger& instance();

    // 日志接口
    void log(Level level, const std::string& msg);

private:
    Logger() = default;
    std::mutex mutex_;
};
} // namespace ts
