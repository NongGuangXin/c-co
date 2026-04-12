#pragma once

#include "file_descriptor.h"

#include <coroutine>
#include <netinet/in.h>
#include <expected>
#include <vector>

class connection {
  public:
    connection() = default;
    explicit connection(FileDescriptor fd): fd_(std::move(fd)) { }

    explicit operator bool() const noexcept {
        return static_cast<bool>(fd_);
    }

    // Awaitable for co_read: 读取一次可用数据
    struct read_awaitable {
        FileDescriptor fd;
        std::vector<unsigned char>& buf;
        std::expected<size_t, int> result;

        bool await_ready() noexcept;
        bool await_suspend(std::coroutine_handle<> h);
        std::expected<size_t, int> await_resume();
    };

    // Awaitable for co_read_until: 持续读取直到填满 buf.size() 字节或对端关闭
    struct read_until_awaitable {
        size_t target;
        size_t total;
        FileDescriptor fd;
        std::vector<unsigned char>& buf;
        std::vector<unsigned char> remain;
        std::expected<size_t, int> result;

        void do_read(std::coroutine_handle<> h);

        bool await_ready() noexcept;
        bool await_suspend(std::coroutine_handle<> h);
        std::expected<size_t, int> await_resume();
    };

    // Awaitable for co_write (handles short writes)
    struct write_awaitable {
        FileDescriptor fd;
        const std::vector<unsigned char>& buf;
        size_t written;
        std::expected<size_t, int> result;

        void do_write(std::coroutine_handle<> h);

        bool await_ready() noexcept;
        bool await_suspend(std::coroutine_handle<> h);
        std::expected<size_t, int> await_resume();
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
        return read_until_awaitable{0, 0, fd_, buf, {}, {}};
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
        connection result;
        FileDescriptor fd;
        struct sockaddr_storage addr;

        bool await_ready() const noexcept;
        bool await_suspend(std::coroutine_handle<> h);
        connection await_resume();
    };

    accept_awaitable co_accept() {
        return accept_awaitable{{}, fd_, {}};
    }

  private:
    FileDescriptor fd_;
};

// Awaitable connect
struct connect_awaitable {
    int port{};
    connection result{};
    FileDescriptor conn_fd{};
    struct sockaddr_in addr{};

    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h);
    connection await_resume();
};

inline connect_awaitable co_connect(int port) {
    return connect_awaitable{port, {}, {}, {}};
}

acceptor co_listen(int port);
