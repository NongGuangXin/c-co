#pragma once

#include "excutor.h"
#include "log.h"

#include <coroutine>
#include <expected>
#include <vector>
#include <cstring>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

class connection {
  public:
    connection() = default;
    explicit connection(FileDescriptor fd): fd_(std::move(fd)) { }

    explicit operator bool() const noexcept {
        return static_cast<bool>(fd_);
    }

    // Awaitable for co_read
    struct read_awaitable {
        FileDescriptor              fd;
        std::vector<unsigned char>& buf;
        std::expected<size_t, int>  result;

        static constexpr size_t kChunkSize = 4096;
        size_t                  total{0};

        bool await_ready() const noexcept {
            return false;
        }

        void do_read(std::coroutine_handle<> h) {
            auto read_cb = [this, h]() mutable {
                for(;;) {
                    if(buf.size() < total + kChunkSize) {
                        buf.resize(total + kChunkSize);
                    }

                    ssize_t n = ::read(
                        fd.handle(), buf.data() + total, buf.size() - total);

                    if(n > 0) {
                        total += static_cast<size_t>(n);
                        continue;
                    }

                    if(n == 0) {
                        buf.resize(total);
                        result = n;
                        h.resume();
                        return;
                    }

                    // n < 0
                    if(errno == EAGAIN || errno == EWOULDBLOCK) {
                        if(total == 0) {
                            do_read(h);
                        } else {
                            buf.resize(total);
                            result = total;
                            h.resume();
                        }
                        return;
                    }

                    buf.resize(total);
                    result = std::unexpected(errno);
                    h.resume();
                    return;
                }
            };

            excutor::instance().register_event(fd, excutor::READ, read_cb);
        }

        void await_suspend(std::coroutine_handle<> h) {
            total = 0;
            do_read(h);
        }

        std::expected<size_t, int> await_resume() {
            return result;
        }
    };

    // Awaitable for co_read_until: 持续读取直到填满 buf.size() 字节或对端关闭
    struct read_until_awaitable {
        FileDescriptor              fd;
        std::vector<unsigned char>& buf;
        size_t                      target;
        size_t                      total{0};
        std::expected<size_t, int>  result;

        bool await_ready() const noexcept {
            return false;
        }

        void do_read(std::coroutine_handle<> h) {
            auto read_cb = [this, h]() mutable {
                for(;;) {
                    ssize_t n =
                        ::read(fd.handle(), buf.data() + total, target - total);

                    if(n > 0) {
                        total += static_cast<size_t>(n);
                        if(total >= target) {
                            result = total;
                            h.resume();
                            return;
                        }
                        continue;
                    }

                    if(n == 0) {
                        buf.resize(total);
                        result = n;
                        h.resume();
                        return;
                    }

                    // n < 0
                    if(errno == EAGAIN || errno == EWOULDBLOCK) {
                        // 内核缓冲区暂时读空，重新注册等待更多数据
                        do_read(h);
                        return;
                    }

                    buf.resize(total);
                    result = std::unexpected(errno);
                    h.resume();
                    return;
                }
            };

            excutor::instance().register_event(fd, excutor::READ, read_cb);
        }

        void await_suspend(std::coroutine_handle<> h) {
            target = buf.size();
            total  = 0;
            do_read(h);
        }

        std::expected<size_t, int> await_resume() {
            return result;
        }
    };

    // Awaitable for co_write (handles short writes)
    struct write_awaitable {
        FileDescriptor                    fd;
        const std::vector<unsigned char>& buf;
        size_t                            written{0};
        std::expected<size_t, int>        result;

        bool await_ready() const noexcept {
            return false;
        }

        void do_write(std::coroutine_handle<> h) {
            excutor::instance().register_event(
                fd, excutor::WRITE, [this, h]() mutable {
                    ssize_t n = ::write(fd.handle(), buf.data() + written,
                        buf.size() - written);
                    if(n < 0) {
                        result = std::unexpected(errno);
                        h.resume();
                        return;
                    }
                    written += static_cast<size_t>(n);
                    if(written < buf.size()) {
                        // 短写，继续注册等待可写
                        do_write(h);
                    } else {
                        result = written;
                        h.resume();
                    }
                });
        }

        void await_suspend(std::coroutine_handle<> h) {
            do_write(h);
        }

        std::expected<size_t, int> await_resume() {
            return result;
        }
    };

    /**
    * 返回值：
    * 1. 成功：读取的字节数
    * 2. 失败：错误码
    * 3. 连接关闭：0
        连接关闭或错误前读取的数据在buf中，可用size()获取实际读取长度
     */
    read_awaitable co_read(std::vector<unsigned char>& buf) {
        return read_awaitable{fd_, buf, {}};
    }

    read_until_awaitable co_read_until(std::vector<unsigned char>& buf) {
        return read_until_awaitable{fd_, buf, 0, 0, {}};
    }

    write_awaitable co_write(const std::vector<unsigned char>& buf) {
        return write_awaitable{fd_, buf, 0, {}};
    }

  private:
    FileDescriptor fd_;
};

class acceptor {
  public:
    acceptor() = default;
    explicit acceptor(FileDescriptor fd): fd_(std::move(fd)) { }

    operator bool() const {
        return static_cast<bool>(fd_);
    }

    struct accept_awaitable {
        FileDescriptor fd;
        connection     result;

        bool await_ready() const noexcept {
            return false;
        }

        void await_suspend(std::coroutine_handle<> h) {
            excutor::instance().register_event(
                fd, excutor::READ, [this, h]() mutable {
                    struct sockaddr_in addr;
                    socklen_t          len       = sizeof(addr);
                    int                client_fd = ::accept4(fd.handle(),
                                       reinterpret_cast<struct sockaddr*>(&addr), &len,
                                       SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if(client_fd < 0) {
                        log::erro("accept4 failed: {}", std::strerror(errno));
                    } else {
                        result = connection(FileDescriptor(client_fd));
                    }
                    h.resume();
                });
        }

        connection await_resume() {
            return std::move(result);
        }
    };

    accept_awaitable co_accept() {
        return accept_awaitable{fd_, {}};
    }

  private:
    FileDescriptor fd_;
};

// Free functions for creating connections
acceptor co_listen(int port);

// Awaitable connect
struct connect_awaitable {
    int        port;
    connection result;
    bool       ready{false};

    bool await_ready() const noexcept {
        return ready;
    }

    // 返回 bool: true 表示不挂起（立即完成），false 表示挂起等待 epoll
    bool await_suspend(std::coroutine_handle<> h);

    connection await_resume() {
        return std::move(result);
    }
};

inline connect_awaitable co_connect(int port) {
    return connect_awaitable{port, {}};
}
