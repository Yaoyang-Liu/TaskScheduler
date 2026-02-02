#include "resource_manager.h"

namespace ts {

    ResourceManager::ResourceManager(ResourceQuota quota) : quota_(quota) {}

    bool ResourceManager::reserve(int cpu, size_t mem_mb) {
        std::scoped_lock lk(mu_);
        if(cpu <= 0 || mem_mb <= 0) {
            return false;
        }
        if(used_cpu + cpu > quota_.total_cpu) {
            return false;
        }
        if(used_mem_mb + mem_mb > quota_.total_mem_mb) {
            return false;
        }
        used_cpu += cpu;
        used_mem_mb += mem_mb;
        return true;
    }

    void ResourceManager::release(int cpu, size_t mem_mb) {
        std::scoped_lock lk(mu_);
        used_cpu -= cpu;
        used_mem_mb -= mem_mb;
        if(used_cpu < 0) {
            used_cpu = 0;
        }
        if(used_mem_mb > quota_.total_mem_mb) {
            used_mem_mb = quota_.total_mem_mb;
        }
    }

    std::pair<int, size_t> ResourceManager::used() const {
        std::scoped_lock lk(mu_);
        return {used_cpu, used_mem_mb};
    }
} // namespace ts
