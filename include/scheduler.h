#pragma once

#include "job.h"

namespace ts {

class Scheduler {
public:
    // 提交任务，返回job id或-1
    int submit(const JobSepc& spec);
    // 启动后台线程
    void start();
    // 停止调度器等待后台退出
    void stop();
};    

} // namespace ts

