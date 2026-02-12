#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <sys/epoll.h>

namespace ts {
class HttpServer {
public:
    using Handler = std::function<std::string()>;

    HttpServer(int port, Handler handler);
    ~HttpServer();

    void start();

    void stop();

private:
    void serve();

    void work_loop();

    void handle_connection(int fd);

    int port_;
    Handler handler_;
    std::atomic<bool> stop_{false};
    std::thread moniter;
    int listen_fd_{-1};
    int epoll_fd_{-1};

    size_t worker_count_{4};
    int recv_timeout_ms_{200};
    size_t max_queue_{100};
    std::vector<std::thread> workers_;
    std::deque<int> conn_queue_;
    std::mutex mtx;
    std::condition_variable queue_cv_;
};
}