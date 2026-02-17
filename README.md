# TaskScheduler

一个用 C++20 实现的高性能任务调度系统，支持资源配额管理、cgroup 隔离和 HTTP API。

## 功能特性

- **优先级调度**：基于优先级的任务队列，支持高优先级任务优先执行
- **资源管理**：CPU 和内存配额管理，支持资源预留和释放
- **cgroup 集成**：通过 cgroup 限制任务资源使用
- **PSI 监控**：实时监控系统压力
- **HTTP API**：提供任务状态查询接口
- **指标统计**：任务运行统计和性能指标

## 编译

```bash
mkdir build && cd build
cmake ..
make
```

## 运行测试

```bash
./scheduler_test
./resource_manager_test
./http_server_test
```

## 使用示例

```cpp
#include "scheduler.h"

int main() {
    ts::SchedulerOptions opts;
    opts.quota = {4, 2048};  // 4 CPU核心, 2GB内存
    opts.max_queue_size = 1000;
    
    ts::Scheduler scheduler(opts);
    scheduler.start();
    
    // 提交任务
    ts::JobSpec spec;
    spec.cmd = "echo hello";
    spec.cpu_cores = 1;
    spec.mem_mb = 256;
    spec.priority = 10;
    
    int job_id = scheduler.submit(spec);
    
    // 获取指标
    auto metrics = scheduler.metrics();
    
    scheduler.stop();
    
    return 0;
}
```

## 项目结构

```
src/
├── main.cpp          # 主程序入口
├── scheduler.cpp     # 调度器实现
├── job.cpp           # 任务实现
├── resource_manager.cpp  # 资源管理器
├── cgroup_helper.cpp # cgroup 辅助函数
├── metrics.cpp       # 指标收集
├── logger.cpp        # 日志系统
└── http_server.cpp   # HTTP 服务器

include/
├── scheduler.h
├── job.h
├── resource_manager.h
├── cgroup_helper.h
├── metrics.h
├── logger.h
├── http_server.h
└── psi_monitor.h

tests/
├── scheduler_test.cpp
├── resource_manager_test.cpp
└── http_server_test.cpp
```

## 依赖

- CMake 3.14+
- C++20 兼容编译器
- Google Test
