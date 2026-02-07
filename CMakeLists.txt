cmake_minimum_required(VERSION 3.14)
project(TaskScheduler)

# 设置C++20标准
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 头文件路径（包含项目头文件和GoogleTest头文件）
include_directories(
    include
    /usr/include/gtest  # 若gtest安装在其他路径，需替换
)

# 编译核心模块（如ResourceManager）
add_library(core
    src/resource_manager.cpp
    src/job.cpp
    src/scheduler.cpp
    src/logger.cpp
    src/metrics.cpp
)

# 编译测试程序
find_package(GTest REQUIRED)
add_executable(resource_manager_test
    tests/resource_manager_test.cpp
)
add_executable(scheduler_test
    tests/scheduler_test.cpp
)
# 链接核心模块、GTest库、线程库
target_link_libraries(resource_manager_test
    core
    ${GTEST_LIBRARIES}
    pthread
)

target_link_libraries(scheduler_test
    core
    ${GTEST_LIBRARIES}
    pthread
)