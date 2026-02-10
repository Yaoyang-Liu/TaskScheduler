#include "scheduler.h"
#include "logger.h"
#include "metrics.h"
#include "cgroup_helper.h"

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
        metrics_.inc_rejected();
        return -1;
    }
    if(!check_blacklist(spec)) {
        Logger::instance().log(Logger::Level::WARN, "submit rejected: cmd blacklist");
        metrics_.inc_rejected();
        return -1;
    }
    if(opts_.max_queue_size > 0 && static_cast<int>(pending_.size()) >= opts_.max_queue_size) {
        Logger::instance().log(Logger::Level::WARN, "submit rejected: queue full");
        metrics_.inc_rejected();
        return -1;
    }

    int id = next_id_++;
    Job job;
    job.id = id;
    job.spec = spec;
    job.status = JobStatus::Pending;
    job.enqueue_time = std::chrono::steady_clock::now();

    pending_.emplace(job);
    metrics_.inc_submitted();
    cv_.notify_all();
    Logger::instance().log(Logger::Level::INFO, "submit id: " + std::to_string(id) + " cmd = " + spec.cmd);
    return id;
}

// start: 启动调度器，创建并启动分发线程和回收线程
void Scheduler::start() {
    is_running_.store(true);
    dispatcher_thread_ = std::thread(&Scheduler::dispatcher_loop, this);
    reaper_thread_ = std::thread(&Scheduler::reaper_loop, this);
    psi_thread_ = std::thread(&Scheduler::psi_loop, this);
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
    if(psi_thread_.joinable()) {
        psi_thread_.join();
    }
}

// idle: 检查调度器是否空闲（无等待任务和无运行任务）
bool Scheduler::idle() const {
    std::scoped_lock lk(mu_);
    return pending_.empty() && running_.empty();
}

// pick_next_job: 从等待队列中选择下一个要执行的任务
// 使用优先级队列，优先选择优先级更高的任务；相同优先级时按入队时间FIFO
bool Scheduler::pick_next_job(Job& out) {
    if(pending_.empty()) {
        return false;
    }
    auto best = pending_.top();
    pending_.pop();
    out = std::move(best);
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
    std::string cg_path = create_cgroup_for_job(job.id, job.spec.cpu_cores, job.spec.mem_mb, opts_.cfg);
    pid_t pid = fork();
    if (pid < 0) {
        Logger::instance().log(Logger::Level::ERROR, "fork failed: " + std::string(strerror(errno)));
        return false;
    }
    if (pid > 0) {
        job.pid = pid;
        job.pgid = pid;
        job.start_time = std::chrono::steady_clock::now();
        setpgid(pid, pid);
        return true;
    }
    else {
        setpgid(0, 0);
        if(!attach_pid_to_cgroup(getpid(), cg_path)) {
            _exit(1);
        }
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
        if (psi_pressure_.load(std::memory_order_relaxed)) {
            metrics_.inc_pressure_blocked();
            lk.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }
        
        Job next_job;
        if (!pick_next_job(next_job)) {
            continue;
        }
        if (!rm_.reserve(next_job.spec.cpu_cores, next_job.spec.mem_mb)) {
            if (opts_.max_queue_size > 0 && static_cast<int>(pending_.size()) >= opts_.max_queue_size) {
                rm_.release(next_job.spec.cpu_cores, next_job.spec.mem_mb);
                Logger::instance().log(Logger::Level::WARN, "job " + std::to_string(next_job.id) + " dropped: queue full");
            } else {
                cv_.wait_for(lk, std::chrono::milliseconds(200));
                pending_.emplace(next_job);
            }
            continue;
        }
        next_job.status = JobStatus::Running;
        running_.emplace(next_job.id, next_job);
        metrics_.inc_running();
        Job& ref = running_.at(next_job.id);
        int64_t wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - ref.enqueue_time).count();
        metrics_.add_queue_wait(wait_ms);
        if (!launch_job(ref)) {
            Logger::instance().log(Logger::Level::ERROR, "failed to launch job id = " + std::to_string(next_job.id));
            rm_.release(next_job.spec.cpu_cores, next_job.spec.mem_mb);
            metrics_.inc_launch_failed();
            metrics_.dec_running();
            running_.erase(next_job.id);
            cv_.notify_all();
            continue;
        }
        cv_.notify_all();
        lk.unlock();
    }
}

// reaper_loop: 回收线程主循环
// 1. 遍历所有运行中的任务
// 2. 检查超时任务，发送 SIGTERM/SIGKILL
// 3. 调用 waitpid 收割已结束的子进程
void Scheduler::reaper_loop() {
    constexpr auto kGracePeriodMs = 500;
    while (is_running_.load() || !running_.empty()) {
        std::unique_lock lk(mu_);
        for (auto it = running_.begin(); it != running_.end();) {
            Job& job = it->second;
            auto now = std::chrono::steady_clock::now();
            // 检查任务是否设置了超时且正在运行
            if (job.spec.timeout_sec > 0 && job.status == JobStatus::Running) {
                auto elapsed = now - job.start_time;
                auto timeout = std::chrono::seconds(job.spec.timeout_sec);
                // 任务执行时间超过超时阈值
                if (elapsed > timeout) {
                    // 首次超时：发送 SIGTERM 优雅终止
                    if (job.sigterm_time.time_since_epoch().count() == 0) {
                        Logger::instance().log(Logger::Level::WARN, "job " + std::to_string(job.id) + " timeout, sending SIGTERM");
                        pid_t target = job.pgid > 0 ? -job.pgid : job.pid;
                        kill(target, SIGTERM);
                        job.sigkill_sent = true;
                        job.sigterm_time = now;
                        cv_.notify_all();
                    } else {
                        // 等待宽限期后强制杀死
                        auto sigterm_elapsed = now - job.sigterm_time;
                        if (sigterm_elapsed > std::chrono::milliseconds(kGracePeriodMs)) {
                            Logger::instance().log(Logger::Level::WARN, "job " + std::to_string(job.id) + " grace period expired, sending SIGKILL");
                            pid_t target = job.pgid > 0 ? -job.pgid : job.pid;
                            kill(job.pid, SIGKILL);
                            cv_.notify_all();
                            continue;
                        }
                    }
                }
            }
            // 非阻塞方式检查子进程状态
            int status = 0;
            pid_t ret = waitpid(job.pid, &status, WNOHANG);
            // 子进程已结束或已回收
            if (ret == job.pid || (ret == -1 && errno == ECHILD)) {
                // 根据退出状态设置任务结果
                if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
                    job.status = JobStatus::Succeeded;
                    metrics_.inc_succeeded();
                } else if (WIFSIGNALED(status)) {
                    job.status = JobStatus::Timeout;
                    metrics_.inc_timeout();
                } else {
                    job.status = JobStatus::Failed;
                    metrics_.inc_failed();
                }
                job.exit_code = status;
                job.end_time = std::chrono::steady_clock::now();
                // 释放占用的资源
                rm_.release(job.spec.cpu_cores, job.spec.mem_mb);
                clean_up_cgroup(opts_.cfg.enabled ? (opts_.cfg.base_path + "/job_" + std::to_string(job.id)) : "");
                metrics_.dec_running();
                Logger::instance().log(Logger::Level::INFO, "job " + std::to_string(job.id) + " finished status=" + std::to_string(status));
                // 从运行队列移除并通知等待线程
                it = running_.erase(it);
                cv_.notify_all();
                break;
            }
            // 短暂休眠避免 busy loop
            lk.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            lk.lock();
        }
    }
}

Metrics::Snapshot Scheduler::metrics() const {
    std::scoped_lock lk(mu_);
    return metrics_.snapshot(static_cast<int>(pending_.size()));
}

static double read_pressure_avg10(const std::string& path, const std::string& token) {
    std::ifstream ifs(path);
    if(!ifs.is_open()) {
        return 0.0;
    }
    std::string line;
    while(getline(ifs, line)) {
        if(line.find(token) == std::string::npos) {
            continue;
        }
        auto pos = line.find("avg10=");
        if(pos == std::string::npos) {
            continue;
        }
        pos += 6;
        std::stringstream ss(line.substr(pos));
        double val = 0.0;
        ss >> val;
        return val; 
    }
    return 0.0;
}

void Scheduler::psi_loop() {
    const double mem_some_th = 0.5;
    const double mem_full_th = 0.1;
    const double cpu_some_th = 0.8;
    auto base = opts_.cfg.base_path;
    auto mem_path = base + "/memory.pressure";
    auto cpu_path = base + "/cpu.pressure";
    bool last_active = false;
    while(is_running_.load()) {
        double mem_some = read_pressure_avg10(mem_path, "some");
        double mem_full = read_pressure_avg10(mem_path, "full");
        double cpu_some = read_pressure_avg10(cpu_path, "some");
        bool active = mem_some > mem_some_th || mem_full > mem_full_th || cpu_some > cpu_some_th;
        psi_pressure_.store(active, std::memory_order_relaxed);
        metrics_.set_pressure_active(active);
        if(active && !last_active) {
            metrics_.inc_pressure_blocked();
            Logger::instance().log(Logger::Level::WARN, "psi_pressure on: mem_some: " + std::to_string(mem_some) + 
                                                        " mem_full: " + std::to_string(mem_full) + 
                                                        " cpu_some: " + std::to_string(cpu_some));
        }
        else if(!active && last_active) {
            Logger::instance().log(Logger::Level::INFO, "psi backpressure off");
        }
        last_active = active;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    psi_pressure_.store(false);
    metrics_.set_pressure_active(false);
}
} // namespace ts
