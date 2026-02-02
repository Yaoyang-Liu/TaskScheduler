#include "logger.h"
#include <chrono>
#include <iomanip>
#include <sstream>
#include <ctime>

namespace ts {
    // 获取 Logger 全局单例：使用局部静态变量保证线程安全初始化
    Logger& Logger::instance() {
        static Logger inst;
        return inst;
    }

    // 线程安全打印日志，附带时间戳与线程 ID。
    void Logger::log(Level level, const std::string& msg) {

        std::scoped_lock lock(mutex_);

        // 获取当前时间
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

        std::stringstream ss;
        ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
        ss << '.' << std::setfill('0') << std::setw(3) << ms.count();

        const char *level_str = level == Level::INFO ? "INFO" : (level == Level::WARN ? "WARN" : "ERROR");
        std::cout << "[" << ss.str() << "] [" << level_str << "] "
                << msg << std::endl;
    }

} // namespace ts