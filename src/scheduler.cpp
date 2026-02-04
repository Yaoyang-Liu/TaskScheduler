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

namespace ts {
    Scheduler::Scheduler(SchedulerOptions opts) : opts_(std::move(opts)), rm_(opts_.quota) {}

    Scheduler::~Scheduler(){ stop(); }

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
        Logger::instance().log(Logger::Level::INFO, "submit id: " + std::to_string(id) + " cmd= " + spec.cmd);
        return id;
    }

    void Scheduler::start() {
        is_running_.store(true);
        dispatcher_thread_ = std::thread(&Scheduler::dispatcher_loop, this);
        reaper_thread_ = std::thread(&Scheduler::reaper_loop, this);
    }

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

    bool Scheduler::idle() const {
        std::scoped_lock lk(mu_);
        return pending_.empty() && running_.empty();
    }

    // 选择下一任务：若启用优先级则取最高优先级，否则 FIFO。
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

    bool Scheduler::check_blacklist(const JobSpec& spec) {
        if(opts_.cmd_blacklist.empty()) {
            return true;
        }
        std::istringstream iss(spec.cmd);
        std::string first;
        iss >> first;
        for(const auto& denied : opts_.cmd_blacklist) {
            if(first == denied) {
                return false;
            }
        }
        return true;
    }

    bool Scheduler::launch_job(Job& job) {
        pid_t pid = fork();
        if(pid > 0) {
            job.pid = pid;
            job.start_time = std::chrono::steady_clock::now();
            return true;
        } else if (pid == 0) {
            // 使用 shell 执行命令（简单实现），若 exec 返回则表示失败
            execl("/bin/sh", "sh", "-c", job.spec.cmd.c_str(), nullptr);
            _exit(127);
        }
        return false;
    }

    void Scheduler::dispatcher_loop() {
        while(is_running_.load()) {
            std::unique_lock lk(mu_);
            cv_.wait(lk, [&] { return !is_running_.load() || !pending_.empty(); });

            if(!is_running_.load()) {
                break;
            }
            if(pending_.empty()) {
                continue;
            }
            Job next_job;
            if(!pick_next_job(next_job)) {
                continue;
            }
            if(!rm_.reserve(next_job.spec.cpu_cores, next_job.spec.mem_mb)) {
                cv_.wait_for(lk, std::chrono::milliseconds(200));
                pending_.push_back(next_job);
                continue;
            }
            next_job.status = JobStatus::Running;
            running_.emplace(next_job.id, next_job);
            if(!launch_job(next_job)) {
                Logger::instance().log(Logger::Level::ERROR, "failed to launch job id = " + std::to_string(next_job.id));
                rm_.release(next_job.spec.cpu_cores, next_job.spec.mem_mb);
                running_.erase(next_job.id);
            }
            lk.unlock();
        }
    }

    void Scheduler::reaper_loop() {
        while (is_running_.load() || !running_.empty()) {
            // 扫描正在运行的任务，处理超时、回收退出的子进程并释放资源
            std::lock_guard lk(mu_);
            for (auto it = running_.begin(); it != running_.end();) {
                Job& job = it->second;
                // 超时处理
                if (job.spec.timeout_sec > 0) {
                    auto elapsed = std::chrono::steady_clock::now() - job.start_time;
                    if (elapsed > std::chrono::seconds(job.spec.timeout_sec)) {
                        kill(job.pid, SIGKILL); // 超过宽限再强杀，整组清理
                        Logger::instance().log(Logger::Level::WARN, "job " + std::to_string(job.id) + " SIGKILL after grace");
                    }
                }

                int status = 0;
                pid_t ret = waitpid(job.pid, &status, WNOHANG);
                if (ret == 0) {
                    ++it;
                    continue;
                }
                if (ret == job.pid) {
                    job.exit_code = status;
                    job.end_time = std::chrono::steady_clock::now();
                    
                    rm_.release(job.spec.cpu_cores, job.spec.mem_mb);
                    Logger::instance().log(Logger::Level::INFO, "job " + std::to_string(job.id) + " finished status=" + std::to_string(status));
                    it = running_.erase(it);
                    cv_.notify_all();
                } else {
                    ++it;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }
} // namespace ts
