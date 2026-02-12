#include "http_server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <cstring>

namespace ts {
    HttpServer::HttpServer(int port, Handler handler) : port_(port), handler_(handler) {}

    HttpServer::~HttpServer() { stop(); }

    void HttpServer::start() {
        if(moniter.joinable()) {
            return;
        }
        stop_.store(false);
        moniter = std::thread(&HttpServer::serve, this);
    }

    void HttpServer::stop() {
        stop_.store(true);
        if(listen_fd_ >= 0) {
            close(listen_fd_);
            listen_fd_ = -1;
        }
        if(epoll_fd_ >= 0) {
            close(epoll_fd_);
            epoll_fd_ = -1;
        }
        {
            std::unique_lock lk(mtx);
            queue_cv_.notify_all();
        }
        if(moniter.joinable()) {
            moniter.join();
        }
        for(auto &w : workers_) {
            if(w.joinable()) {
                w.join();
            }
        }
        std::unique_lock lk(mtx);
        while(!conn_queue_.empty()) {
            ::close(conn_queue_.front());
            conn_queue_.pop_front();
        }
    }

    static bool set_nonblock(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        if(flags < 0) {
            return false;
        }
        return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
    }

    static std::string parse_path(int fd) {
        std::string data;
        char buf[1024];
        while(data.find("\r\n\r\n") == std::string::npos && data.size() < 4096) {
            ssize_t n = recv(fd, buf, sizeof(buf), 0);
            if(n <= 0) {
                break;
            }
            data.append(buf, buf + n);
            if(n < static_cast<ssize_t>(sizeof(buf))) {
                break;
            }
        }
        auto pos = data.find("\r\n");
        if(pos == std::string::npos) {
            return {};
        }
        std::string line = data.substr(0, pos);
        auto sp1 = line.find(' ');
        if(sp1 == std::string::npos) {
            return {};
        }
        auto sp2 = line.find(' ', sp1 + 1);
        if(sp2 == std::string::npos) {
            return {};
        }
        return line.substr(sp1 + 1, sp2 - sp1 - 1);
    }

    void HttpServer::work_loop() {
        while(true) {
            int fd = -1;
            {
                std::unique_lock lk(mtx);
                queue_cv_.wait(lk, [&] { return stop_.load() || !conn_queue_.empty(); });
                if(conn_queue_.empty()) {
                    if(stop_.load()) {
                        break;
                    }
                    continue;
                }
                fd = conn_queue_.front();
                conn_queue_.pop_front();
            }
            if(fd >= 0) {
                handle_connection(fd);
            }
        }
    }

    void HttpServer::handle_connection(int fd) {
        // 设置收发超时，避免慢连接拖垮线程
        timeval tv{recv_timeout_ms_ / 1000, (recv_timeout_ms_ % 1000) * 1000};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        std::string path = parse_path(fd);
        std::string body;
        int status = 200;
        std::string content_type = "text/plain; charset=utf-8";
        if(path.empty()) {
            status = 400;
            body = "bad request";
        }
        else if(path == "/health") {
            body = "ok";
        }
        else if(path == "/metrics") {
            body = handler_ ? handler_() : "";
        }
        else {
            status = 404;
            body = "not found";
        }
        std::string resp = "HTTP/1.1 " + std::to_string(status) + " " + (status == 200 ? "OK" : (status == 400 ? "Bad Request" : "Not Found")) +
                           "\r\nContent-Type: " + content_type +
                           "\r\nConnection: close\r\nContent-Length: " + std::to_string(body.size()) +
                           "\r\n\r\n" + body;
        send(fd, resp.data(), resp.size(), 0);
        close(fd);
    }

    void HttpServer::serve() {
        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if(listen_fd_ < 0) {
            return;
        }
        int opt = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        if(bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
            close(listen_fd_);
            listen_fd_ = -1;
            return;
        }
        if(!set_nonblock(listen_fd_)) {
            close(listen_fd_);
            listen_fd_ = -1;
            return;
        }
        if(listen(listen_fd_, 64) < 0) {
            close(listen_fd_);
            listen_fd_ = -1;
            return;
        }

        epoll_fd_ = epoll_create1(0);
        if(epoll_fd_ < 0) {
            close(listen_fd_);
            listen_fd_ = -1;
            return;
        }
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = listen_fd_;
        if(epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0) {
            close(epoll_fd_);
            epoll_fd_ = -1;
            close(listen_fd_);
            listen_fd_ = -1;
            return;
        }

        size_t worker_n = worker_count_ > 0 ? worker_count_ : 4;
        workers_.reserve(worker_n);
        for(size_t i = 0; i < worker_n; ++i) {
            workers_.emplace_back(&HttpServer::work_loop, this);
        }

        constexpr int MAX_EVENTS = 8;
        epoll_event events[MAX_EVENTS];
        while(!stop_.load()) {
            int nfds = epoll_wait(epoll_fd_, events, MAX_EVENTS, 1000);
            if(nfds <= 0) {
                continue;
            }
            for(int i = 0; i < nfds; ++i) {
                if(events[i].data.fd != listen_fd_) {
                    continue;
                }
                while(true) {
                    int conn = accept(listen_fd_, nullptr, nullptr);
                    if(conn < 0) {
                        if(errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        }
                        if(errno == EINTR) {
                            continue;
                        }
                        break;
                    }
                    {
                        std::lock_guard lk(mtx);
                        if(conn_queue_.size() >= max_queue_) {
                            const char *resp = "HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
                            send(conn, resp, strlen(resp), 0);
                            close(conn);
                            continue;
                        }
                        conn_queue_.push_back(conn);
                    }
                    queue_cv_.notify_one();
                }
            }
        }
        queue_cv_.notify_all();
    }
}