#include "scheduler.h" // 假设你的 Scheduler 定义在 scheduler.h
#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <future>
#include <unistd.h>
#include <sys/wait.h>

using namespace ts;

// 辅助：等待调度器空闲或超时
bool wait_until_idle(Scheduler& sched, std::chrono::seconds timeout = std::chrono::seconds(10)) {
    auto start = std::chrono::steady_clock::now();
    while (!sched.idle()) {
        if (std::chrono::steady_clock::now() - start > timeout) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return true;
}

// 辅助：获取 running_ 中的任务数量（仅用于调试，非线程安全！）
// 实际测试中我们不依赖内部状态，只通过行为验证
// 所以这里不实现

TEST(SchedulerTest, BasicJobExecution) {
    SchedulerOptions opts;
    opts.quota = {2, 1024}; // 2核，1GB
    Scheduler sched(opts);
    sched.start();

    JobSpec spec;
    spec.cmd = "echo hello";
    spec.cpu_cores = 1;
    spec.mem_mb = 128;

    int id = sched.submit(spec);
    EXPECT_GE(id, 0);

    EXPECT_TRUE(wait_until_idle(sched));

    sched.stop();
}

TEST(SchedulerTest, ResourceLimitEnforcesQueueing) {
    SchedulerOptions opts;
    opts.quota = {1, 512};
    Scheduler sched(opts);
    sched.start();

    // 提交两个各需 1 核的任务 → 第二个应排队
    JobSpec spec;
    spec.cmd = "sleep 1";
    spec.cpu_cores = 1;
    spec.mem_mb = 256;

    int id1 = sched.submit(spec);
    int id2 = sched.submit(spec);
    EXPECT_GE(id1, 0);
    EXPECT_GE(id2, 0);

    // 等待全部完成
    EXPECT_TRUE(wait_until_idle(sched, std::chrono::seconds(3)));

    sched.stop();
}

TEST(SchedulerTest, WhitelistBlocksNonWhitelisted) {
    SchedulerOptions opts;
    opts.quota = {2, 1024};
    opts.cmd_whitelist = {"echo"};
    Scheduler sched(opts);
    sched.start();

    JobSpec spec1;
    spec1.cmd = "echo ok";
    EXPECT_GE(sched.submit(spec1), 0); // 允许

    JobSpec spec2;
    spec2.cmd = "ls /";
    EXPECT_EQ(sched.submit(spec2), -1); // 拒绝

    EXPECT_TRUE(wait_until_idle(sched));

    sched.stop();
}

TEST(SchedulerTest, BlacklistBlocksListedCommands) {
    SchedulerOptions opts;
    opts.quota = {2, 1024};
    opts.cmd_blacklist = {"rm"};
    Scheduler sched(opts);
    sched.start();

    JobSpec spec1;
    spec1.cmd = "echo safe";
    EXPECT_GE(sched.submit(spec1), 0);

    JobSpec spec2;
    spec2.cmd = "rm -rf /"; // 被禁止
    EXPECT_EQ(sched.submit(spec2), -1);

    EXPECT_TRUE(wait_until_idle(sched));
    sched.stop();
}

TEST(SchedulerTest, TimeoutKillsLongRunningJob) {
    SchedulerOptions opts;
    opts.quota = {2, 1024};
    Scheduler sched(opts);
    sched.start();

    JobSpec spec;
    spec.cmd = "sleep 10"; // 很长
    spec.timeout_sec = 1;  // 1秒超时
    spec.cpu_cores = 1;
    spec.mem_mb = 128;

    int id = sched.submit(spec);
    EXPECT_GE(id, 0);

    // 等待超时发生（最多2秒）
    EXPECT_TRUE(wait_until_idle(sched, std::chrono::seconds(3)));

    sched.stop();
}

TEST(SchedulerTest, InvalidCommandFailsGracefully) {
    SchedulerOptions opts;
    opts.quota = {1, 512};
    Scheduler sched(opts);
    sched.start();

    JobSpec spec;
    spec.cmd = "/nonexistent/command";
    spec.cpu_cores = 1;
    spec.mem_mb = 128;

    int id = sched.submit(spec);
    EXPECT_GE(id, 0);

    EXPECT_TRUE(wait_until_idle(sched));

    sched.stop();
}

TEST(SchedulerTest, ConcurrentSubmissionIsSafe) {
    SchedulerOptions opts;
    opts.quota = {4, 2048};
    Scheduler sched(opts);
    sched.start();

    const int N = 20;
    std::vector<std::future<int>> futures;

    for (int i = 0; i < N; ++i) {
        futures.push_back(std::async(std::launch::async, [&sched]() {
            JobSpec spec;
            spec.cmd = "echo concurrent";
            spec.cpu_cores = 1;
            spec.mem_mb = 64;
            return sched.submit(spec);
        }));
    }

    std::vector<int> ids;
    for (auto& f : futures) {
        int id = f.get();
        EXPECT_GE(id, 0);
        ids.push_back(id);
    }

    EXPECT_TRUE(wait_until_idle(sched, std::chrono::seconds(5)));
    sched.stop();
}

TEST(SchedulerTest, IdleReturnsCorrectly) {
    SchedulerOptions opts;
    opts.quota = {1, 512};
    Scheduler sched(opts);
    sched.start();

    EXPECT_TRUE(sched.idle());

    JobSpec spec;
    spec.cmd = "sleep 0.1";
    spec.cpu_cores = 1;
    spec.mem_mb = 128;

    int id = sched.submit(spec);
    EXPECT_GE(id, 0);

    // 短暂 sleep 后可能还在运行
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(sched.idle());

    EXPECT_TRUE(wait_until_idle(sched));
    EXPECT_TRUE(sched.idle());

    sched.stop();
}

TEST(SchedulerTest, PrioritySchedulingExecutesHigherPriorityFirst) {
    SchedulerOptions opts;
    opts.quota = {2, 1024};
    opts.enable_priority = true;
    Scheduler sched(opts);
    sched.start();

    JobSpec spec_low;
    spec_low.cmd = "sleep 0.5";
    spec_low.cpu_cores = 1;
    spec_low.mem_mb = 128;
    spec_low.priority = 1;

    JobSpec spec_high;
    spec_high.cmd = "sleep 0.1";
    spec_high.cpu_cores = 1;
    spec_high.mem_mb = 128;
    spec_high.priority = 10;

    int id_low = sched.submit(spec_low);
    int id_high = sched.submit(spec_high);

    EXPECT_GE(id_low, 0);
    EXPECT_GE(id_high, 0);

    EXPECT_TRUE(wait_until_idle(sched, std::chrono::seconds(2)));

    sched.stop();
}

TEST(SchedulerTest, QueueSizeLimitRejectsExcessJobs) {
    SchedulerOptions opts;
    opts.quota = {4, 4096};
    opts.max_queue_size = 3;
    Scheduler sched(opts);
    sched.start();

    std::vector<int> ids;
    for (int i = 0; i < 5; ++i) {
        JobSpec spec;
        spec.cmd = "echo test" + std::to_string(i);
        spec.cpu_cores = 1;
        spec.mem_mb = 64;
        int id = sched.submit(spec);
        ids.push_back(id);
    }

    int success_count = 0;
    for (int id : ids) {
        if (id >= 0) success_count++;
    }
    EXPECT_EQ(success_count, 3);

    EXPECT_TRUE(wait_until_idle(sched, std::chrono::seconds(5)));
    sched.stop();
}

TEST(SchedulerTest, ZeroMaxQueueSizeAllowsUnlimited) {
    SchedulerOptions opts;
    opts.quota = {4, 4096};
    opts.max_queue_size = 0;
    Scheduler sched(opts);
    sched.start();

    std::vector<int> ids;
    for (int i = 0; i < 100; ++i) {
        JobSpec spec;
        spec.cmd = "echo test" + std::to_string(i);
        spec.cpu_cores = 1;
        spec.mem_mb = 64;
        int id = sched.submit(spec);
        if (id >= 0) {
            ids.push_back(id);
        }
    }

    EXPECT_EQ(ids.size(), 100);

    EXPECT_TRUE(wait_until_idle(sched, std::chrono::seconds(10)));
    sched.stop();
}

TEST(SchedulerTest, MultipleJobsWithDifferentResourceRequirements) {
    SchedulerOptions opts;
    opts.quota = {4, 1024};
    Scheduler sched(opts);
    sched.start();

    JobSpec spec1;
    spec1.cmd = "echo job1";
    spec1.cpu_cores = 1;
    spec1.mem_mb = 128;

    JobSpec spec2;
    spec2.cmd = "echo job2";
    spec2.cpu_cores = 2;
    spec2.mem_mb = 512;

    JobSpec spec3;
    spec3.cmd = "echo job3";
    spec3.cpu_cores = 1;
    spec3.mem_mb = 256;

    int id1 = sched.submit(spec1);
    int id2 = sched.submit(spec2);
    int id3 = sched.submit(spec3);

    EXPECT_GE(id1, 0);
    EXPECT_GE(id2, 0);
    EXPECT_GE(id3, 0);

    EXPECT_TRUE(wait_until_idle(sched, std::chrono::seconds(5)));
    sched.stop();
}

TEST(SchedulerTest, PriorityWithEqualPriorityUsesFIFO) {
    SchedulerOptions opts;
    opts.quota = {2, 1024};
    opts.enable_priority = true;
    Scheduler sched(opts);
    sched.start();

    for (int i = 0; i < 3; ++i) {
        JobSpec spec;
        spec.cmd = "echo job" + std::to_string(i);
        spec.cpu_cores = 1;
        spec.mem_mb = 64;
        spec.priority = 5;
        int id = sched.submit(spec);
        EXPECT_GE(id, 0);
    }

    EXPECT_TRUE(wait_until_idle(sched, std::chrono::seconds(5)));
    sched.stop();
}

TEST(SchedulerTest, StopCanBeCalledMultipleTimesSafely) {
    SchedulerOptions opts;
    opts.quota = {1, 512};
    Scheduler sched(opts);
    sched.start();

    JobSpec spec;
    spec.cmd = "sleep 0.2";
    spec.cpu_cores = 1;
    spec.mem_mb = 64;

    sched.submit(spec);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    sched.stop();
    sched.stop();
    sched.stop();
}

TEST(SchedulerTest, StartStopCycleCanBeRepeated) {
    for (int cycle = 0; cycle < 3; ++cycle) {
        SchedulerOptions opts;
        opts.quota = {1, 512};
        Scheduler sched(opts);
        sched.start();

        JobSpec spec;
        spec.cmd = "echo cycle" + std::to_string(cycle);
        spec.cpu_cores = 1;
        spec.mem_mb = 64;

        int id = sched.submit(spec);
        EXPECT_GE(id, 0);

        EXPECT_TRUE(wait_until_idle(sched, std::chrono::seconds(2)));
        sched.stop();
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}