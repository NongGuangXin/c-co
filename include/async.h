#pragma once

#include "excutor.h"
#include "log.h"

#include <coroutine>
#include <cstring>
#include <expected>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

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

        bool await_ready() const noexcept {
            return false;
        }

        void await_suspend(std::coroutine_handle<> h) {
            excutor::instance().register_event(
                fd, excutor::READ, [this, h]() mutable {
                    ssize_t n = ::read(fd.handle(), buf.data(), buf.size());
                    if(n < 0) {
                        result = std::unexpected(errno);
                    } else {
                        result = static_cast<size_t>(n);
                    }
                    h.resume();
                });
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

    read_awaitable co_read(std::vector<unsigned char>& buf) {
        return read_awaitable{fd_, buf, {}};
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
acceptor   co_listen(int port);
connection co_connect_sync(int port); // helper

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
