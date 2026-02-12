#pragma once

#include "job.h"
#include "resource_manager.h"
#include "metrics.h"
#include "cgroup_helper.h"
#include <vector>
#include <unordered_map>
#include <atomic>
#include <condition_variable>
#include <thread>
#include <queue>

namespace ts {

struct SchedulerOptions {
    ResourceQuota quota; // 资源配额设置
    CgroupConfig cfg; // cgroup 配置
    int max_queue_size{1000}; // 最大等待队列大小
    std::vector<std::string> cmd_whitelist; // 允许执行的命令白名单
    std::vector<std::string> cmd_blacklist; // 禁止执行的命令黑名单
};

/**
 * @brief 任务比较器
 * 优先级高的任务先执行，优先级相同时按入队时间早的先执行
 */
struct JobComparator {
    bool operator()(const Job& a, const Job& b) const {
        if (a.spec.priority != b.spec.priority) {
            return a.spec.priority < b.spec.priority; // 优先级降序排列
        }
        return a.enqueue_time > b.enqueue_time; // 入队时间升序排列
    }
};

class Scheduler {
public:
    explicit Scheduler(SchedulerOptions opts);  ///< 使用指定配置构造调度器
    ~Scheduler();                                 ///< 停止调度器并清理资源

    /**
     * @brief 提交任务
     * @param spec 任务规格
     * @return int 任务ID，成功返回正整数，失败返回-1
     */
    int submit(const JobSpec& spec);
    
    // 启动后台调度线程
    void start();
    
    // 停止调度器并等待后台线程退出
    void stop();
    
    // 检查是否无正在运行的任务
    bool idle() const;

    // 获取当前指标快照
    Metrics::Snapshot metrics() const; 

private:
    bool pick_next_job(Job& out); // 从队列中选择下一个要执行的任务
    bool check_whitelist(const JobSpec& spec); // 检查任务是否在白名单中
    bool check_blacklist(const JobSpec& spec); // 检查任务是否在黑名单中
    bool launch_job(Job& job); // 启动任务进程
    void dispatcher_loop(); // 调度主循环
    void reaper_loop(); // 回收子进程状态
    void psi_loop(); // psi背压测试循环

    SchedulerOptions opts_; // 调度器配置选项
    ResourceManager rm_; // 资源管理器
    std::priority_queue<Job, std::vector<Job>, JobComparator> pending_; // 待执行任务队列
    std::unordered_map<int, Job> running_; // 运行中任务映射 (job_id -> Job)
    mutable std::mutex mu_; // 互斥锁保护共享数据
    int next_id_{1}; // 下一个分配的任务ID
    std::atomic<bool> is_running_; // 调度器运行状态标志
    std::atomic<bool> psi_pressure_; // psi背压标志
    std::thread dispatcher_thread_; // 调度线程
    std::thread reaper_thread_; // 进程回收线程
    std::thread psi_thread_; // psi背压测试线程
    std::condition_variable cv_; // 条件变量用于线程同步
    Metrics metrics_; // 指标统计器
};

} // namespace ts
