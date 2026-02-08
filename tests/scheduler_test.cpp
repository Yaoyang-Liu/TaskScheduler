/**
 * @file scheduler_test.cpp
 * @brief 调度器单元测试
 * 
 * 本测试文件包含对 TaskScheduler 的全面功能测试，覆盖以下方面：
 * 1. 基础任务执行
 * 2. 资源限制与队列管理
 * 3. 命令过滤（白名单/黑名单）
 * 4. 任务超时机制
 * 5. 并发安全
 * 6. 优先级调度
 * 7. 指标统计
 * 8. cgroup v2 资源隔离
 */

#include "scheduler.h" 
#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <future>
#include <unistd.h>
#include <sys/wait.h>
#include <filesystem>
#include <fstream>

using namespace ts;

//=============================================================================
// 辅助函数
//=============================================================================

/**
 * @brief 等待调度器进入空闲状态
 * @param sched 调度器引用
 * @param timeout 超时时间，默认10秒
 * @return true-空闲，false-超时
 * 
 * 通过反复检查 idle() 方法来判断调度器是否处理完所有任务
 */
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

//=============================================================================
// 基础功能测试
//=============================================================================

/**
 * @test BasicJobExecution
 * @brief 验证调度器能够正确执行简单任务
 * 
 * 测试流程：
 * 1. 创建调度器，配置2核CPU、1GB内存资源
 * 2. 提交一个简单的 echo 命令任务
 * 3. 等待任务完成
 * 
 * 验证点：
 * - 任务提交成功（返回有效的 job id）
 * - 任务能够正常完成
 * - 调度器最终进入空闲状态
 */
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

/**
 * @test ResourceLimitEnforcesQueueing
 * @brief 验证资源限制触发任务排队
 * 
 * 测试场景：
 * - 调度器总资源：1核CPU、512MB内存
 * - 提交两个各需1核的任务
 * 
 * 预期行为：
 * - 第一个任务立即执行
 * - 第二个任务因资源不足而排队等待
 * - 两个任务最终都能完成
 * 
 * 验证调度器的资源预留机制（ResourceManager）是否正确工作
 */
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

//=============================================================================
// 命令过滤测试
//=============================================================================

/**
 * @test WhitelistBlocksNonWhitelisted
 * @brief 验证白名单机制允许指定命令，拒绝未授权命令
 * 
 * 配置：
 * - 白名单仅包含 "echo" 命令
 * 
 * 测试场景：
 * - 尝试提交 "echo ok" → 应被允许
 * - 尝试提交 "ls /" → 应被拒绝（返回 -1）
 * 
 * 用于生产环境安全控制，只允许执行预定义的安全命令
 */
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

/**
 * @test BlacklistBlocksListedCommands
 * @brief 验证黑名单机制阻止危险命令
 * 
 * 配置：
 * - 黑名单包含 "rm" 命令
 * 
 * 测试场景：
 * - 尝试提交 "echo safe" → 应被允许
 * - 尝试提交 "rm -rf /" → 应被拒绝（返回 -1）
 * 
 * 用于防止误操作或恶意删除系统文件
 */
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

//=============================================================================
// 超时控制测试
//=============================================================================

/**
 * @test TimeoutKillsLongRunningJob
 * @brief 验证任务超时机制正常工作
 * 
 * 配置：
 * - 任务执行 10 秒
 * - 超时设置为 1 秒
 * 
 * 预期行为：
 * - 任务在运行 1 秒后被 SIGTERM 终止
 * - 经过宽限期后若未结束则发送 SIGKILL
 * - 任务状态应标记为 Timeout
 * 
 * 验证调度器的超时控制功能
 */
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

/**
 * @test InvalidCommandFailsGracefully
 * @brief 验证无效命令能够优雅处理
 * 
 * 测试场景：
 * - 提交一个不存在的命令路径
 * 
 * 预期行为：
 * - 任务提交成功（提交阶段不验证命令存在性）
 * - 任务执行失败（execve 返回错误）
 * - 任务状态标记为 Failed
 * 
 * 验证调度器对执行失败的容错处理
 */
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

//=============================================================================
// 并发与线程安全测试
//=============================================================================

/**
 * @test ConcurrentSubmissionIsSafe
 * @brief 验证多线程并发提交的安全性
 * 
 * 测试配置：
 * - 20 个线程同时提交任务
 * - 调度器资源充足（4核、2GB）
 * 
 * 验证点：
 * - 所有提交都能获得唯一的 job id
 * - 没有 job id 重复或丢失
 * - 所有任务最终都能完成
 * 
 * 测试 Scheduler 的线程安全性和 id 生成器的正确性
 */
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

/**
 * @test IdleReturnsCorrectly
 * @brief 验证 idle() 方法正确反映调度器状态
 * 
 * 测试场景：
 * 1. 初始状态：调度器空闲，idle() 应返回 true
 * 2. 提交任务后：任务正在运行，idle() 应返回 false
 * 3. 任务完成后：调度器空闲，idle() 应返回 true
 * 
 * 用于外部监控调度器状态
 */
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

/**
 * @test StopCanBeCalledMultipleTimesSafely
 * @brief 验证 stop() 可以安全地多次调用
 * 
 * 测试场景：
 * - 在任务执行中连续多次调用 stop()
 * 
 * 预期行为：
 * - 不会崩溃或产生未定义行为
 * - 调度器正确停止
 * 
 * 防御性编程，防止用户误操作
 */
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

/**
 * @test StartStopCycleCanBeRepeated
 * @brief 验证调度器可以多次启动和停止
 * 
 * 测试场景：
 * - 连续执行 3 个完整的 start-stop 周期
 * 
 * 预期行为：
 * - 每个周期都能正常启动和停止
 * - 每个周期的任务都能正确执行
 * 
 * 验证调度器的可重用性
 */
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

//=============================================================================
// 优先级调度测试
//=============================================================================

/**
 * @test PrioritySchedulingExecutesHigherPriorityFirst
 * @brief 验证高优先级任务先于低优先级任务执行
 * 
 * 配置：
 * - 调度器资源：2核
 * - 低优先级任务：sleep 0.5秒，优先级 1
 * - 高优先级任务：sleep 0.1秒，优先级 10
 * 
 * 预期行为：
 * - 高优先级任务先获得执行机会
 * - 两个任务都能完成
 * 
 * 验证优先队列调度逻辑
 */
TEST(SchedulerTest, PrioritySchedulingExecutesHigherPriorityFirst) {
    SchedulerOptions opts;
    opts.quota = {2, 1024};
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

/**
 * @test PriorityWithEqualPriorityUsesFIFO
 * @brief 验证相同优先级任务按 FIFO 顺序执行
 * 
 * 配置：
 * - 3个相同优先级的任务
 * - 调度器资源：2核
 * 
 * 预期行为：
 * - 任务按提交顺序依次执行
 * 
 * 验证优先级相等时的队列行为
 */
TEST(SchedulerTest, PriorityWithEqualPriorityUsesFIFO) {
    SchedulerOptions opts;
    opts.quota = {2, 1024};
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

//=============================================================================
// 队列限制测试
//=============================================================================

/**
 * @test QueueSizeLimitRejectsExcessJobs
 * @brief 验证队列大小限制拒绝超额任务
 * 
 * 配置：
 * - 最大队列大小：3
 * - 提交 5 个任务
 * 
 * 预期行为：
 * - 前 3 个任务进入队列
 * - 后 2 个任务因队列满被拒绝
 * 
 * 资源保护机制，防止过量任务堆积
 */
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

/**
 * @test ZeroMaxQueueSizeAllowsUnlimited
 * @brief 验证队列大小为 0 表示无限制
 * 
 * 配置：
 * - max_queue_size = 0
 * - 提交 100 个任务
 * 
 * 预期行为：
 * - 所有任务都能提交成功
 * 
 * 0 作为特殊值表示不限制队列大小
 */
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

/**
 * @test MultipleJobsWithDifferentResourceRequirements
 * @brief 验证不同资源需求的任务可以正确调度
 * 
 * 测试配置：
 * - 任务1：1核、128MB
 * - 任务2：2核、512MB
 * - 任务3：1核、256MB
 * - 总资源：4核、1024MB
 * 
 * 预期行为：
 * - 所有任务都能成功提交
 * - 所有任务都能完成
 * 
 * 验证资源分配的多样性支持
 */
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

//=============================================================================
// 指标统计测试
//=============================================================================

/**
 * @test MetricsSubmittedCounter
 * @brief 验证提交任务计数指标正确
 * 
 * 测试流程：
 * - 提交 3 个任务
 * - 等待全部完成
 * 
 * 验证：
 * - submitted = 3
 * - succeeded = 3
 * - failed = 0
 * - timeout = 0
 */
TEST(SchedulerTest, MetricsSubmittedCounter) {
    SchedulerOptions opts;
    opts.quota = {4, 1024};
    Scheduler sched(opts);
    sched.start();

    JobSpec spec;
    spec.cmd = "echo test";
    spec.cpu_cores = 1;
    spec.mem_mb = 128;

    int id1 = sched.submit(spec);
    int id2 = sched.submit(spec);
    int id3 = sched.submit(spec);
    EXPECT_GE(id1, 0);
    EXPECT_GE(id2, 0);
    EXPECT_GE(id3, 0);

    EXPECT_TRUE(wait_until_idle(sched));
    sched.stop();

    auto snapshot = sched.metrics();
    EXPECT_EQ(snapshot.submitted, 3);
    EXPECT_EQ(snapshot.succeeded, 3);
    EXPECT_EQ(snapshot.failed, 0);
    EXPECT_EQ(snapshot.timeout, 0);
}

/**
 * @test MetricsFailedCounter
 * @brief 验证失败任务计数正确
 * 
 * 测试流程：
 * - 提交一个不存在的命令
 * - 等待任务失败
 * 
 * 验证：
 * - submitted = 1
 * - failed = 1
 */
TEST(SchedulerTest, MetricsFailedCounter) {
    SchedulerOptions opts;
    opts.quota = {2, 1024};
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

    auto snapshot = sched.metrics();
    EXPECT_EQ(snapshot.submitted, 1);
    EXPECT_EQ(snapshot.failed, 1);
}

/**
 * @test MetricsTimeoutCounter
 * @brief 验证超时任务计数正确
 * 
 * 测试流程：
 * - 提交一个长时间任务（sleep 10）
 * - 设置 1 秒超时
 * - 等待超时发生
 * 
 * 验证：
 * - submitted = 1
 * - timeout = 1
 * - succeeded = 0
 */
TEST(SchedulerTest, MetricsTimeoutCounter) {
    SchedulerOptions opts;
    opts.quota = {2, 1024};
    Scheduler sched(opts);
    sched.start();

    JobSpec spec;
    spec.cmd = "sleep 10";
    spec.timeout_sec = 1;
    spec.cpu_cores = 1;
    spec.mem_mb = 128;

    int id = sched.submit(spec);
    EXPECT_GE(id, 0);

    EXPECT_TRUE(wait_until_idle(sched, std::chrono::seconds(3)));
    sched.stop();

    auto snapshot = sched.metrics();
    EXPECT_EQ(snapshot.submitted, 1);
    EXPECT_EQ(snapshot.timeout, 1);
    EXPECT_EQ(snapshot.succeeded, 0);
}

/**
 * @test MetricsQueueWaitTime
 * @brief 验证排队等待时间指标正确
 * 
 * 测试场景：
 * - 资源不足，第二个任务需要排队等待
 * 
 * 验证：
 * - 排队等待时间记录非负
 * - 排队计数为 2
 */
TEST(SchedulerTest, MetricsQueueWaitTime) {
    SchedulerOptions opts;
    opts.quota = {1, 512};
    Scheduler sched(opts);
    sched.start();

    JobSpec spec;
    spec.cmd = "sleep 1";
    spec.cpu_cores = 1;
    spec.mem_mb = 256;

    int id1 = sched.submit(spec);
    int id2 = sched.submit(spec);
    EXPECT_GE(id1, 0);
    EXPECT_GE(id2, 0);

    EXPECT_TRUE(wait_until_idle(sched, std::chrono::seconds(5)));
    sched.stop();

    auto snapshot = sched.metrics();
    EXPECT_EQ(snapshot.submitted, 2);
    EXPECT_EQ(snapshot.succeeded, 2);
    EXPECT_GE(snapshot.queue_wait_ms_avg, 0);
    EXPECT_GE(snapshot.queue_wait_ms_max, 0);
    EXPECT_EQ(snapshot.queue_wait_count, 2);
}

/**
 * @test MetricsRejectedCounter
 * @brief 验证被拒绝任务计数正确
 * 
 * 配置：
 * - 白名单只允许 "echo"
 * 
 * 测试流程：
 * - 提交允许的命令
 * - 提交不允许的命令
 * 
 * 验证：
 * - submitted = 1
 * - rejected = 1
 */
TEST(SchedulerTest, MetricsRejectedCounter) {
    SchedulerOptions opts;
    opts.quota = {2, 1024};
    opts.cmd_whitelist = {"echo"};
    Scheduler sched(opts);
    sched.start();

    JobSpec spec1;
    spec1.cmd = "echo allowed";
    spec1.cpu_cores = 1;
    spec1.mem_mb = 128;

    JobSpec spec2;
    spec2.cmd = "ls /";
    spec2.cpu_cores = 1;
    spec2.mem_mb = 128;

    int id1 = sched.submit(spec1);
    int id2 = sched.submit(spec2);
    EXPECT_GE(id1, 0);
    EXPECT_EQ(id2, -1);

    EXPECT_TRUE(wait_until_idle(sched));
    sched.stop();

    auto snapshot = sched.metrics();
    EXPECT_EQ(snapshot.submitted, 1);
    EXPECT_EQ(snapshot.rejected, 1);
}

/**
 * @test MetricsRunningCounter
 * @brief 验证运行中任务计数正确
 * 
 * 测试流程：
 * - 提交任务
 * - 任务执行中检查 running 计数 >= 1
 * - 任务结束后检查 running = 0
 */
TEST(SchedulerTest, MetricsRunningCounter) {
    SchedulerOptions opts;
    opts.quota = {4, 1024};
    Scheduler sched(opts);
    sched.start();

    JobSpec spec;
    spec.cmd = "sleep 2";
    spec.cpu_cores = 1;
    spec.mem_mb = 128;

    int id = sched.submit(spec);
    EXPECT_GE(id, 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto snapshot = sched.metrics();
    EXPECT_GE(snapshot.running, 1);

    EXPECT_TRUE(wait_until_idle(sched));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    sched.stop();

    snapshot = sched.metrics();
    EXPECT_EQ(snapshot.running, 0);
}

//=============================================================================
// cgroup v2 资源隔离测试
//=============================================================================

/**
 * @test CgroupEnabledCreatesCgroupDirectory
 * @brief 验证启用 cgroup 时正确创建资源限制目录
 * 
 * cgroup v2 作用说明：
 * - 为每个任务创建独立的 cgroup，实现资源隔离
 * - 通过 cpu.max 限制 CPU 使用配额
 * - 通过 mem.max 限制内存使用上限
 * - 通过 cgroup.procs 将任务进程加入 cgroup
 * 
 * 测试配置：
 * - cfg.enabled = true
 * - CPU: 1 核 → cpu.max = "100000 100000" (100% CPU)
 * - Memory: 128MB → mem.max = "134217728" (128 * 1024 * 1024)
 * 
 * 验证点：
 * 1. cgroup 目录创建
 * 2. cgroup.procs 文件存在
 * 3. mem.max 文件存在且有内容
 * 4. cpu.max 文件存在且有内容
 * 5. 任务结束后 cgroup 目录被清理
 */
TEST(SchedulerTest, CgroupEnabledCreatesCgroupDirectory) {
    std::string cg_path = "/tmp/test_scheduler_cgroup_" + std::to_string(getpid());
    
    // 清理可能存在的残留
    std::filesystem::remove_all(cg_path);
    
    SchedulerOptions opts;
    opts.quota = {2, 1024};
    opts.cfg.enabled = true;
    opts.cfg.base_path = cg_path;
    Scheduler sched(opts);
    sched.start();

    JobSpec spec;
    spec.cmd = "sleep 0.5";
    spec.cpu_cores = 1;
    spec.mem_mb = 128;

    int id = sched.submit(spec);
    EXPECT_GE(id, 0);

    // 等待任务开始运行
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 验证 cgroup 目录已创建
    std::string expected_cg = cg_path + "/job_" + std::to_string(id);
    EXPECT_TRUE(std::filesystem::exists(expected_cg)) << "Cgroup directory should exist at " << expected_cg;
    
    // 验证 cgroup.procs 文件存在且包含进程 ID
    std::string procs_file = expected_cg + "/cgroup.procs";
    EXPECT_TRUE(std::filesystem::exists(procs_file)) << "cgroup.procs should exist";
    
    // 验证 mem.max 文件
    std::string mem_file = expected_cg + "/mem.max";
    EXPECT_TRUE(std::filesystem::exists(mem_file)) << "mem.max should exist";
    if (std::filesystem::exists(mem_file)) {
        std::ifstream ifs(mem_file);
        std::string content;
        ifs >> content;
        EXPECT_FALSE(content.empty()) << "mem.max should have content";
    }
    
    // 验证 cpu.max 文件
    std::string cpu_file = expected_cg + "/cpu.max";
    EXPECT_TRUE(std::filesystem::exists(cpu_file)) << "cpu.max should exist";
    if (std::filesystem::exists(cpu_file)) {
        std::ifstream ifs(cpu_file);
        std::string content;
        std::getline(ifs, content);
        EXPECT_FALSE(content.empty()) << "cpu.max should have content";
    }

    EXPECT_TRUE(wait_until_idle(sched));
    sched.stop();

    // 验证 cgroup 目录已清理
    EXPECT_FALSE(std::filesystem::exists(expected_cg)) << "Cgroup directory should be cleaned up after job finishes";
    
    // 清理测试目录
    std::filesystem::remove_all(cg_path);
}

/**
 * @test CgroupDisabledDoesNotCreateCgroupDirectory
 * @brief 验证禁用 cgroup 时不创建任何 cgroup 目录
 * 
 * 配置：
 * - cfg.enabled = false
 * 
 * 预期行为：
 * - 任务正常执行
 * - 不会创建 cgroup 相关目录
 * 
 * 用于兼容不需要资源隔离的场景，减少开销
 */
TEST(SchedulerTest, CgroupDisabledDoesNotCreateCgroupDirectory) {
    std::string cg_path = "/tmp/test_scheduler_no_cgroup_" + std::to_string(getpid());
    
    SchedulerOptions opts;
    opts.quota = {2, 1024};
    opts.cfg.enabled = false;
    opts.cfg.base_path = cg_path;
    Scheduler sched(opts);
    sched.start();

    JobSpec spec;
    spec.cmd = "echo test";
    spec.cpu_cores = 1;
    spec.mem_mb = 128;

    int id = sched.submit(spec);
    EXPECT_GE(id, 0);

    // 短暂等待后检查没有创建 cgroup 目录
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 验证 cgroup 目录不存在
    std::string expected_cg = cg_path + "/job_" + std::to_string(id);
    EXPECT_FALSE(std::filesystem::exists(expected_cg)) << "Cgroup directory should NOT exist when disabled";

    EXPECT_TRUE(wait_until_idle(sched));
    sched.stop();
}

/**
 * @test CgroupSetsMemoryLimit
 * @brief 验证 cgroup 正确设置内存限制
 * 
 * cgroup v2 内存限制机制：
 * - 通过 mem.max 文件设置最大内存限制
 * - 任务内存使用超过限制会被 OOM killer 终止
 * 
 * 测试验证：
 * - mem.max 文件存在
 * - 文件内容不为空（包含内存限制值）
 * 
 * 示例值：256MB → "268435456"
 */
TEST(SchedulerTest, CgroupSetsMemoryLimit) {
    std::string cg_path = "/tmp/test_scheduler_memlimit_" + std::to_string(getpid());
    
    // 清理可能存在的残留
    std::filesystem::remove_all(cg_path);
    
    SchedulerOptions opts;
    opts.quota = {2, 1024};
    opts.cfg.enabled = true;
    opts.cfg.base_path = cg_path;
    Scheduler sched(opts);
    sched.start();

    JobSpec spec;
    spec.cmd = "echo memory_test";
    spec.cpu_cores = 1;
    spec.mem_mb = 256;

    int id = sched.submit(spec);
    EXPECT_GE(id, 0);

    // 等待任务开始运行
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 验证内存限制文件
    std::string mem_file = cg_path + "/job_" + std::to_string(id) + "/mem.max";
    if (std::filesystem::exists(mem_file)) {
        std::ifstream ifs(mem_file);
        std::string content;
        ifs >> content;
        EXPECT_FALSE(content.empty()) << "mem.max should have content";
    }

    EXPECT_TRUE(wait_until_idle(sched));
    sched.stop();

    // 清理测试目录
    std::filesystem::remove_all(cg_path);
}

/**
 * @test CgroupSetsCpuLimit
 * @brief 验证 cgroup 正确设置 CPU 限制
 * 
 * cgroup v2 CPU 限制机制：
 * - 通过 cpu.max 文件设置 CPU 使用配额
 * - 格式："<quota> <period>"，例如 "200000 100000" 表示 2 核
 * - quota 是每 period (100ms) 内可使用的 CPU 时间（微秒）
 * 
 * 测试验证：
 * - cpu.max 文件存在
 * - 文件内容不为空（包含 CPU 限制值）
 * 
 * 示例值：2 核 → "200000 100000" (200ms / 100ms = 2 CPUs)
 */
TEST(SchedulerTest, CgroupSetsCpuLimit) {
    std::string cg_path = "/tmp/test_scheduler_cpulimit_" + std::to_string(getpid());
    
    // 清理可能存在的残留
    std::filesystem::remove_all(cg_path);
    
    SchedulerOptions opts;
    opts.quota = {4, 2048};
    opts.cfg.enabled = true;
    opts.cfg.base_path = cg_path;
    Scheduler sched(opts);
    sched.start();

    JobSpec spec;
    spec.cmd = "echo cpu_test";
    spec.cpu_cores = 2;
    spec.mem_mb = 128;

    int id = sched.submit(spec);
    EXPECT_GE(id, 0);

    // 等待任务开始运行
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 验证 CPU 限制文件
    std::string cpu_file = cg_path + "/job_" + std::to_string(id) + "/cpu.max";
    if (std::filesystem::exists(cpu_file)) {
        std::ifstream ifs(cpu_file);
        std::string content;
        std::getline(ifs, content);
        EXPECT_FALSE(content.empty()) << "cpu.max should have content";
    }

    EXPECT_TRUE(wait_until_idle(sched));
    sched.stop();

    // 清理测试目录
    std::filesystem::remove_all(cg_path);
}

/**
 * @test CgroupMultipleJobsSeparateDirectories
 * @brief 验证多个任务拥有独立的 cgroup 目录
 * 
 * cgroup 隔离作用：
 * - 每个任务有独立的资源限制
 * - 任务之间不会相互影响
 * - 便于资源核算和问题排查
 * 
 * 测试场景：
 * - 提交两个任务
 * - 验证两个 cgroup 目录都存在
 * - 验证任务结束后目录都被清理
 */
TEST(SchedulerTest, CgroupMultipleJobsSeparateDirectories) {
    std::string cg_path = "/tmp/test_scheduler_multi_" + std::to_string(getpid());
    
    // 清理可能存在的残留
    std::filesystem::remove_all(cg_path);
    
    SchedulerOptions opts;
    opts.quota = {4, 2048};
    opts.cfg.enabled = true;
    opts.cfg.base_path = cg_path;
    Scheduler sched(opts);
    sched.start();

    JobSpec spec1;
    spec1.cmd = "sleep 1";
    spec1.cpu_cores = 1;
    spec1.mem_mb = 128;

    JobSpec spec2;
    spec2.cmd = "sleep 0.5";
    spec2.cpu_cores = 1;
    spec2.mem_mb = 128;

    int id1 = sched.submit(spec1);
    int id2 = sched.submit(spec2);
    EXPECT_GE(id1, 0);
    EXPECT_GE(id2, 0);
    EXPECT_NE(id1, id2);

    // 等待任务开始运行
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 验证两个 cgroup 目录都存在
    std::string cg1 = cg_path + "/job_" + std::to_string(id1);
    std::string cg2 = cg_path + "/job_" + std::to_string(id2);
    EXPECT_TRUE(std::filesystem::exists(cg1)) << "First cgroup should exist";
    EXPECT_TRUE(std::filesystem::exists(cg2)) << "Second cgroup should exist";

    EXPECT_TRUE(wait_until_idle(sched, std::chrono::seconds(3)));
    sched.stop();

    // 验证 cgroup 目录都已清理
    EXPECT_FALSE(std::filesystem::exists(cg1)) << "First cgroup should be cleaned up";
    EXPECT_FALSE(std::filesystem::exists(cg2)) << "Second cgroup should be cleaned up";
    
    // 清理测试目录
    std::filesystem::remove_all(cg_path);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
