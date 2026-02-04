#include "resource_manager.h"
#include "scheduler.h"
#include "logger.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <gtest/gtest.h>

using namespace ts;

// 资源预留与释放应保持正确计数。
TEST(ResourceManagerTest, ReserveRelease) {
    // 目标：验证 reserve/release 的计数正确性与边界行为（资源不足时应拒绝）。
    ResourceManager rm(ResourceQuota{4, 1024});
    EXPECT_TRUE(rm.reserve(2, 256));
    auto [cpu, mem] = rm.used();
    EXPECT_EQ(cpu, 2);
    EXPECT_EQ(mem, 256u);

    EXPECT_FALSE(rm.reserve(3, 900)); // 超过 CPU/内存
    rm.release(2, 256);
    auto [cpu2, mem2] = rm.used();
    EXPECT_EQ(cpu2, 0);
    EXPECT_EQ(mem2, 0u);
}

// 队列上限满时，后续提交应被拒绝。
TEST(SchedulerTest, QueueLimitRejects) {
    // 目标：验证 max_queue_size 生效；队列满后 submit 返回 -1 并计入 rejected。
    SchedulerOptions opts{
        .quota = ResourceQuota{4, 1024},
        .max_queue_size = 1,
        .enable_priority = false,
        .cmd_whitelist = {},
        .cmd_blacklist = {},
    };
    Scheduler sched(opts);
    EXPECT_GT(sched.submit(JobSpec{"true", 1, 128, 0}), 0);
    // 第二个提交应因队列上限被拒绝
    EXPECT_LT(sched.submit(JobSpec{"true", 1, 128, 0}), 0);
}

// 非白名单命令应被拒绝。
TEST(SchedulerTest, WhitelistRejects) {
    // 目标：验证 whitelist 生效；命令首 token 不在白名单则拒绝。
    SchedulerOptions opts{
        .quota = ResourceQuota{4, 1024},
        .max_queue_size = 10,
        .enable_priority = false,
        .cmd_whitelist = {"ls"},
        .cmd_blacklist = {},
    };
    Scheduler sched(opts);
    // 非白名单命令应被拒绝
    EXPECT_LT(sched.submit(JobSpec{"echo hi", 1, 128, 0}), 0);
}

// // 指标端点应返回 Prometheus 文本字段。
// TEST(SchedulerTest, MetricsHttpEndpoint) {
//     // 目标：验证内置 MetricsHttpServer 能响应 HTTP 请求并返回指标文本。
//     constexpr int kPort = 18080;
//     SchedulerOptions opts{
//         .quota = ResourceQuota{4, 1024},
//         .max_queue_size = 10,
//         .enable_priority = false,
//         .cmd_whitelist = {},
//         .cmd_blacklist = {},
//     };
//     Scheduler sched(opts);
//     sched.start();
//     EXPECT_GT(sched.submit(JobSpec{"true", 1, 128, 0}), 0);
//     while (!sched.idle()) {
//         std::this_thread::sleep_for(std::chrono::milliseconds(50));
//     }
//     // 拉取 HTTP 指标
//     int fd = socket(AF_INET, SOCK_STREAM, 0);
//     ASSERT_GE(fd, 0);
//     sockaddr_in addr{};
//     addr.sin_family = AF_INET;
//     addr.sin_port = htons(kPort);
//     addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
//     ASSERT_EQ(connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
//     // 说明：MetricsHttpServer 约定在 /metrics 路径返回 Prometheus 文本。
//     const char req[] = "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n";
//     send(fd, req, sizeof(req) - 1, 0);
//     // 循环读取直到对端关闭连接，避免只读到 HTTP 头导致断言失败。
//     std::string resp;
//     char buf[512];
//     while (true) {
//         ssize_t n = recv(fd, buf, sizeof(buf), 0);
//         if (n <= 0) break;
//         resp.append(buf, buf + n);
//         if (resp.size() > 64 * 1024) break; // 防御性上限，避免异常响应无限增长
//     }
//     close(fd);
//     ASSERT_FALSE(resp.empty());
//     // 断言：应包含核心指标字段（以当前实现为准）。
//     EXPECT_NE(resp.find("tasks_total"), std::string::npos);
//     EXPECT_NE(resp.find("tasks_running_current"), std::string::npos);
//     sched.stop();
// }

// 高优先级任务应先于低优先级执行。
TEST(SchedulerTest, PriorityOrderHighFirst) {
    // 目标：验证 enable_priority 模式下，高 priority 的任务优先出队并先执行。
    const char* outfile = "/tmp/ts_priority_order.txt";
    unlink(outfile);
    SchedulerOptions opts{
        .quota = ResourceQuota{1, 1024}, // 限制为 1，确保顺序可见
        .max_queue_size = 10,
        .enable_priority = true,
        .cmd_whitelist = {"echo"},
        .cmd_blacklist = {},
    };
    Scheduler sched(opts);
    sched.start();
    // 先提交低优先级，再提交高优先级，期望高优先级先执行
    EXPECT_GT(sched.submit(JobSpec{"echo low >> /tmp/ts_priority_order.txt", 1, 128, 0, 0}), 0);
    EXPECT_GT(sched.submit(JobSpec{"echo high >> /tmp/ts_priority_order.txt", 1, 128, 0, 10}), 0);
    while (!sched.idle()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    sched.stop();

    FILE* f = fopen(outfile, "r");
    ASSERT_NE(f, nullptr);
    char buf[64];
    std::string content;
    while (fgets(buf, sizeof(buf), f)) {
        content += buf;
    }
    fclose(f);
    // 期望第一行是 high
    ASSERT_FALSE(content.empty());
    EXPECT_NE(content.find("high"), std::string::npos);
    EXPECT_LT(content.find("high"), content.find("low"));
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}