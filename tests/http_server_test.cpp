/**
 * @file http_server_test.cpp
 * @brief HttpServer 单元测试
 */

#include "http_server.h"
#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <iostream>
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace ts;

namespace {

int send_http_request(const std::string& host, int port, const std::string& path) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if(sock < 0) {
        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if(inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        close(sock);
        return -1;
    }

    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    if(connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }

    std::string request = "GET " + path + " HTTP/1.1\r\nHost: " + host + ":" + std::to_string(port) + "\r\nConnection: close\r\n\r\n";
    send(sock, request.c_str(), request.size(), 0);

    char buf[4096];
    int total = 0;
    while(true) {
        ssize_t n = recv(sock, buf + total, sizeof(buf) - total - 1, 0);
        if(n <= 0) break;
        total += n;
        if(total >= (int)sizeof(buf) - 1) break;
    }
    buf[total] = '\0';
    close(sock);

    if(total == 0) {
        return -1;
    }

    std::string response(buf);
    if(response.find("HTTP/1.1 200") != std::string::npos) {
        return 200;
    } else if(response.find("HTTP/1.1 404") != std::string::npos) {
        return 404;
    } else if(response.find("HTTP/1.1 400") != std::string::npos) {
        return 400;
    }
    return -1;
}

} // namespace

TEST(HttpServerTest, StartAndStop) {
    HttpServer server(18080, nullptr);
    server.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    server.stop();
}

TEST(HttpServerTest, HealthEndpoint) {
    HttpServer server(18081, nullptr);
    server.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    int status = send_http_request("127.0.0.1", 18081, "/health");
    EXPECT_EQ(status, 200);

    server.stop();
}

TEST(HttpServerTest, MetricsEndpoint) {
    std::atomic<bool> called{false};
    HttpServer server(18082, [&]() {
        called = true;
        return "test_metrics";
    });
    server.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    int status = send_http_request("127.0.0.1", 18082, "/metrics");
    EXPECT_EQ(status, 200);
    EXPECT_TRUE(called);

    server.stop();
}

TEST(HttpServerTest, NotFoundEndpoint) {
    HttpServer server(18083, nullptr);
    server.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    int status = send_http_request("127.0.0.1", 18083, "/nonexistent");
    EXPECT_NE(status, -1);

    server.stop();
}

TEST(HttpServerTest, MultipleRequests) {
    HttpServer server(18084, nullptr);
    server.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    for(int i = 0; i < 5; ++i) {
        int status = send_http_request("127.0.0.1", 18084, "/health");
        EXPECT_EQ(status, 200);
    }

    server.stop();
}

TEST(HttpServerTest, ConcurrentRequests) {
    HttpServer server(18085, nullptr);
    server.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::atomic<int> success{0};
    std::vector<std::thread> threads;
    for(int i = 0; i < 10; ++i) {
        threads.emplace_back([&success]() {
            int status = send_http_request("127.0.0.1", 18085, "/health");
            if(status == 200) success++;
        });
    }
    for(auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(success, 10);

    server.stop();
}

TEST(HttpServerTest, StartStopCycle) {
    for(int cycle = 0; cycle < 3; ++cycle) {
        HttpServer server(18086 + cycle, nullptr);
        server.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        int status = send_http_request("127.0.0.1", 18086 + cycle, "/health");
        EXPECT_EQ(status, 200);

        server.stop();
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
