#pragma once

#include <mutex>
#include <optional>

namespace ts {

struct ResourceQuota {
   int total_cpu{4};
   size_t total_mem_mb{2048}; 
};

class ResourceManager {
public:
    explicit ResourceManager(ResourceQuota quota);

    bool reserve(int cpu, size_t mem_mb);

    void release(int cpu, size_t mem_mb);

    ResourceQuota quota() const {return quota_;};

    std::pair<int, size_t> used() const;
private:
    ResourceQuota quota_;
    int used_cpu{0};
    int used_mem_mb{0};
    mutable std::mutex mu_;
};

} // namespace ts

