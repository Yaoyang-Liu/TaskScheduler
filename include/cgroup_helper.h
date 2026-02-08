#pragma once

#include <string>
#include <cstddef>

namespace ts {
struct CgroupConfig {
    bool enabled{false};
    std::string base_path{"/sys/fs/cgroup/scheduler"};
};

std::string create_cgroup_for_job(int job_id, int cpu_cores, int mem_mb, const CgroupConfig& cfg);

bool attach_pid_to_cgroup(pid_t pid, const std::string& cg_path);

void clean_up_cgroup(const std::string& cg_path);
} // namespace ts
