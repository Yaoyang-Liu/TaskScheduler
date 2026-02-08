#pragma once

#include <atomic>
#include <thread>
#include <chrono>
#include <string>
#include <fstream>
#include <optional>

namespace ts {

struct PSILevels {
    double cpu{0.0};
    double memory{0.0};
    double io{0.0};
};

struct PSIData {
    double avg10{0.0};
    double avg60{0.0};
    double avg300{0.0};
    double total{0.0};
};

class PSIMonitor {
public:
    explicit PSIMonitor(std::chrono::seconds interval = std::chrono::seconds(5));
    ~PSIMonitor();

    void start();
    void stop();

    PSILevels current_levels() const;
    PSIData get_cpu() const;
    PSIData get_memory() const;
    PSIData get_io() const;

    static bool is_available();

private:
    void read_loop();
    std::optional<PSIData> read_psi_file(const std::string& path);

    std::atomic<bool> running_{false};
    std::thread monitor_thread_;
    std::chrono::seconds interval_;

    std::atomic<PSILevels> current_levels_;

    std::atomic<PSIData> cpu_data_;
    std::atomic<PSIData> memory_data_;
    std::atomic<PSIData> io_data_;
};

} // namespace ts
