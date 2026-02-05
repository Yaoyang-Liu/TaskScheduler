#include "scheduler.h"
#include "logger.h"
#include <csignal>
#include <sys/wait.h>
#include <sys/resource.h>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <chrono>
#include <thread>
#include <filesystem>
#include <cstring>
#include <cerrno>

namespace ts {

// 构造函数：使用给定的选项初始化调度器，创建资源管理器
Scheduler::Scheduler(SchedulerOptions opts) : opts_(std::move(opts)), rm_(opts_.quota) {}

// 析构函数：确保停止调度器
Scheduler::~Scheduler(){ stop(); }

// submit: 提交任务到调度器，返回任务ID（失败返回-1）
// 包含白名单检查、黑名单检查、队列大小限制
int Scheduler::submit(const JobSpec& spec) {
    std::scoped_lock lk(mu_);
    if(!check_whitelist(spec)) {
        Logger::instance().log(Logger::Level::WARN, "submit rejected: cmd not allowed");
        return -1;
    }
    if(!check_blacklist(spec)) {
        Logger::instance().log(Logger::Level::WARN, "submit rejected: cmd blacklist");
        return -1;
    }
    if(opts_.max_queue_size > 0 && static_cast<int>(pending_.size()) >= opts_.max_queue_size) {
        Logger::instance().log(Logger::Level::WARN, "submit rejected: queue full");
        return -1;
    }

    int id = next_id_++;
    Job job;
    job.id = id;
    job.spec = spec;
    job.status = JobStatus::Pending;
    job.enqueue_time = std::chrono::steady_clock::now();

    pending_.push_back(job);
    cv_.notify_all();
    Logger::instance().log(Logger::Level::INFO, "submit id: " + std::to_string(id) + " cmd = " + spec.cmd);
    return id;
}

// start: 启动调度器，创建并启动分发线程和回收线程
void Scheduler::start() {
    is_running_.store(true);
    dispatcher_thread_ = std::thread(&Scheduler::dispatcher_loop, this);
    reaper_thread_ = std::thread(&Scheduler::reaper_loop, this);
}

// stop: 停止调度器，设置停止标志并等待线程退出
void Scheduler::stop() {
    is_running_.store(false);
    cv_.notify_all();
    if(dispatcher_thread_.joinable()) {
        dispatcher_thread_.join();
    }
    if(reaper_thread_.joinable()) {
        reaper_thread_.join();
    }
}

// idle: 检查调度器是否空闲（无等待任务和无运行任务）
bool Scheduler::idle() const {
    std::scoped_lock lk(mu_);
    return pending_.empty() && running_.empty();
}

// pick_next_job: 从等待队列中选择下一个要执行的任务
// 如果启用优先级调度，选择优先级最高的任务；否则FIFO
bool Scheduler::pick_next_job(Job& out) {
    if(pending_.empty()) {
        return false;
    }
    if(!opts_.enable_priority) {
        out = pending_.front();
        pending_.erase(pending_.begin());
        return true;
    }
    auto best_it = pending_.begin();
    for(auto it = pending_.begin(); it != pending_.end(); ++it) {
        if(it->spec.priority > best_it->spec.priority) {
            best_it = it;
        }
    }
    out = *best_it;
    pending_.erase(best_it);
    return true;
}

// check_whitelist: 检查命令是否在白名单中
bool Scheduler::check_whitelist(const JobSpec& spec) {
    if(opts_.cmd_whitelist.empty()) {
        return true;
    }
    std::istringstream iss(spec.cmd);
    std::string first;
    iss >> first;
    if(first.empty()) {
        return false;
    }
    for(const auto& allowed : opts_.cmd_whitelist) {
        if(first == allowed) {
            return true;
        }
    }
    return false;
}

// check_blacklist: 检查命令是否在黑名单中（如果在则拒绝）
bool Scheduler::check_blacklist(const JobSpec& spec) {
    if (opts_.cmd_blacklist.empty()) {
        return true;
    }
    std::istringstream iss(spec.cmd);
    std::string first;
    iss >> first;
    for (const auto& denied : opts_.cmd_blacklist) {
        if(first == denied) {
            return false;
        }
    }
    return true;
}

// launch_job: 通过fork和exec执行任务
// 父进程：记录PID和启动时间
// 子进程：通过/bin/sh执行命令
bool Scheduler::launch_job(Job& job) {
    pid_t pid = fork();
    if (pid < 0) {
        Logger::instance().log(Logger::Level::ERROR, "fork failed: " + std::string(strerror(errno)));
        return false;
    }
    if (pid > 0) {
        job.pid = pid;
        job.start_time = std::chrono::steady_clock::now();
        return true;
    }
    else {
        execl("/bin/sh", "sh", "-c", job.spec.cmd.c_str(), nullptr);
        _exit(127);
    }
}

// dispatcher_loop: 分发线程主循环
// 1. 等待新任务到达
// 2. 选择下一个任务
// 3. 检查资源是否足够
// 4. 启动任务并加入运行队列
void Scheduler::dispatcher_loop() {
    while (is_running_.load()) {
        std::unique_lock lk(mu_);
        cv_.wait(lk, [&] { return !is_running_.load() || !pending_.empty(); });

        if (!is_running_.load()) {
            break;
        }
        if (pending_.empty()) {
            continue;
        }
        Job next_job;
        if (!pick_next_job(next_job)) {
            continue;
        }
        if (!rm_.reserve(next_job.spec.cpu_cores, next_job.spec.mem_mb)) {
            cv_.wait_for(lk, std::chrono::milliseconds(200));
            pending_.push_back(next_job);
            continue;
        }
        next_job.status = JobStatus::Running;
        if (!launch_job(next_job)) {
            Logger::instance().log(Logger::Level::ERROR, "failed to launch job id = " + std::to_string(next_job.id));
            rm_.release(next_job.spec.cpu_cores, next_job.spec.mem_mb);
            continue;
        }
        running_.emplace(next_job.id, next_job);
        lk.unlock();
    }
}

// reaper_loop: 回收线程主循环，负责：
// 1. 检查运行中任务的超时情况
// 2. 回收已结束的子进程
// 3. 释放资源并更新任务状态
void Scheduler::reaper_loop() {
    while (is_running_.load() || !running_.empty()) {
        std::unique_lock lk(mu_);
        for (auto it = running_.begin(); it != running_.end();) {
            Job& job = it->second;
            if (job.spec.timeout_sec > 0) {
                auto elapsed = std::chrono::steady_clock::now() - job.start_time;
                if (elapsed > std::chrono::seconds(job.spec.timeout_sec)) {
                    if (kill(job.pid, SIGKILL) == -1 && errno != ESRCH) {
                        Logger::instance().log(Logger::Level::ERROR, "kill failed: " + std::string(strerror(errno)));
                    }
                    Logger::instance().log(Logger::Level::WARN, "job " + std::to_string(job.id) + " SIGKILL after timeout");
                }
            }

            int status = 0;
            pid_t ret = waitpid(job.pid, &status, WNOHANG);
            if (ret == 0) {
                ++it;
                continue;
            }
            if (ret == job.pid) {
                // 正常结束：检查退出状态
                if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
                    job.status = JobStatus::Succeeded;
                }
                else if (WIFSIGNALED(status)) {
                    job.status = JobStatus::Timeout;
                }
                else {
                    job.status = JobStatus::Failed;
                }
                job.exit_code = status;
                job.end_time = std::chrono::steady_clock::now();

                rm_.release(job.spec.cpu_cores, job.spec.mem_mb);
                Logger::instance().log(Logger::Level::INFO, "job " + std::to_string(job.id) + " finished status=" + std::to_string(status));
                it = running_.erase(it);
                cv_.notify_all();
            } else if (ret == -1 && errno == ECHILD) {
                // 子进程已被其他方式回收（可能是SIGKILL导致）
                Logger::instance().log(Logger::Level::WARN, "job " + std::to_string(job.id) + " child already reaped");
                it = running_.erase(it);
            } else {
                ++it;
            }
        }
        lk.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}
} // namespace ts
