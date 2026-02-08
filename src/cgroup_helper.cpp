#include "cgroup_helper.h"
#include <filesystem>
#include <fstream>
#include <iostream>

namespace ts {
    std::string create_cgroup_for_job(int job_id, int cpu_cores, int mem_mb, const CgroupConfig& cfg) {
        if(!cfg.enabled) {
            return {};
        }
        std::string cg_path = cfg.base_path + "/job_" + std::to_string(job_id);
        try {
            // 创建父目录（如果不存在）
            std::filesystem::create_directories(cfg.base_path);
            std::filesystem::create_directory(cg_path);

            std::ofstream mem_limit(cg_path + "/mem.max");
            mem_limit << (mem_mb * 1024 * 1024);

            std::ofstream cpu_max(cg_path + "/cpu.max");
            cpu_max << (cpu_cores * 100000) << " 100000";

            return cg_path;
        } catch(std::exception &e) {
            std::cerr << "create cgroup for job failed: " << e.what() << std::endl;
            return {};
        }
    }

    bool attach_pid_to_cgroup(pid_t pid, const std::string& cg_path) {
        if(cg_path.empty()) {
            return true;
        }
        try {
            std::ofstream procs(cg_path + "/cgroup.procs");
            procs << pid;
            return true;
        } catch(std::exception &e) {
            std::cerr << "attach pid to cgroup failed: " << e.what() << std::endl;
            return false;
        }
    }

    void clean_up_cgroup(const std::string& cg_path) {
        if(cg_path.empty()) {
            return;
        }
        std::error_code ec;
        std::filesystem::remove_all(cg_path, ec);
    }
}