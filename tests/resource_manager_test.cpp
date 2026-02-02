// tests/resource_manager_test.cpp
#include <gtest/gtest.h>
#include "resource_manager.h"

namespace ts {
// 测试用例1：资源足够时能成功预留
TEST(ResourceManagerTest, ReserveSuccess) {
    ResourceQuota quota{4, 2048};
    ResourceManager rm(quota);
    // 预留2核CPU + 512MB内存
    EXPECT_TRUE(rm.reserve(2, 512));
    auto [used_cpu, used_mem] = rm.used();
    EXPECT_EQ(used_cpu, 2);
    EXPECT_EQ(used_mem, 512);
}

// 测试用例2：资源不足时预留失败
TEST(ResourceManagerTest, ReserveFailed) {
    ResourceQuota quota{4, 2048};
    ResourceManager rm(quota);
    // 先预留3核 + 1024MB
    rm.reserve(3, 1024);
    // 再预留2核 + 1024MB：CPU和内存都不够
    EXPECT_FALSE(rm.reserve(2, 1024));
}

// 测试用例3：释放资源后能再次预留
TEST(ResourceManagerTest, ReleaseAndReserve) {
    ResourceQuota quota{4, 2048};
    ResourceManager rm(quota);
    rm.reserve(4, 2048);
    // 资源耗尽，预留失败
    EXPECT_FALSE(rm.reserve(1, 1));
    // 释放所有资源
    rm.release(4, 2048);
    // 再次预留成功
    EXPECT_TRUE(rm.reserve(2, 1024));
}
}
int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}