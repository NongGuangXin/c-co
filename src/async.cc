#include "async.h"
#include "co_excutor.h"
#include "log.h"

#include <cerrno>
#include <cstring>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <vector>
#include <algorithm>

// -----------------------------------------------------------------------
// read_awaitable: 使用 co_excutor async_io READ 读取一次可用数据
// -----------------------------------------------------------------------

bool connection::read_awaitable::await_ready() const noexcept {
    return false;
}

bool connection::read_awaitable::await_suspend(std::coroutine_handle<> h) {
    co_excutor::instance().async_io(co_excutor::CO_EVENT::READ, fd.handle(),
        buf, [this, h](int res) mutable {
            if(res > 0) {
                result = static_cast<size_t>(res);
            } else if(res == 0) {
                result = static_cast<size_t>(0);
            } else {
                result = std::unexpected(-res);
            }
            h.resume();
            return;
        });
    return true;
}

std::expected<size_t, int> connection::read_awaitable::await_resume() {
    return result;
}

// -----------------------------------------------------------------------
// read_until_awaitable: 持续读取直到填满buf
// -----------------------------------------------------------------------

void connection::read_until_awaitable::do_read(std::coroutine_handle<> h) {
    ssize_t need = static_cast<ssize_t>(target - total);

    co_excutor::io_callback_t read_cb = [this, h](int res) mutable {
        if(res > 0) {
            size_t bytes_read = static_cast<size_t>(res);

            std::copy(remain.begin(), remain.begin() + bytes_read,
                buf.begin() + total);
            total += bytes_read;
            if(total >= target) {
                result = total;
                h.resume();
                return;
            }

            // 未读满，继续提交
            remain.resize(target - total);
            do_read(h);
            return;
        } else if(res == 0) {
            result = 0;
            h.resume();
            return;
        } else { // 错误
            result = std::unexpected(-res);
            h.resume();
        }
        return;
    };

    // 使用 async_io 读取一次可用数据
    // 如果读取的数据不够，则继续提交读取请求
    // 如果读取的数据够，则返回
    // 如果读取的数据不够，且遇到 EOF/错误，则返回
    co_excutor::instance().async_io(co_excutor::CO_EVENT::READ, fd.handle(),
        remain, std::move(read_cb), need);
}

bool connection::read_until_awaitable::await_ready() const noexcept {
    return false;
}

bool connection::read_until_awaitable::await_suspend(
    std::coroutine_handle<> h) {
    remain.resize(buf.size());
    target = buf.size();
    total  = 0;

    do_read(h);
    return true;
}

std::expected<size_t, int> connection::read_until_awaitable::await_resume() {
    return result;
}

// -----------------------------------------------------------------------
// write_awaitable: 使用 co_excutor async_io WRITE 写入全部数据（处理短写）
// -----------------------------------------------------------------------

void connection::write_awaitable::do_write(std::coroutine_handle<> h) {
    co_excutor::io_callback_t write_cb = [this, h](int res) mutable {
        if(res < 0) {
            result = std::unexpected(-res);
            h.resume();
            return;
        }

        written += static_cast<size_t>(res);
        if(written >= buf.size()) {
            result = written;
            h.resume();
            return;
        }

        do_write(h);
        return;
    };

    co_excutor::instance().async_io(co_excutor::CO_EVENT::WRITE, fd.handle(),
        const_cast<std::vector<unsigned char>&>(buf), write_cb, written);
}

bool connection::write_awaitable::await_ready() const noexcept {
    return false;
}

bool connection::write_awaitable::await_suspend(std::coroutine_handle<> h) {
    do_write(h);
    return true;
}

std::expected<size_t, int> connection::write_awaitable::await_resume() {
    return std::move(result);
}

// -----------------------------------------------------------------------
// accept_awaitable: 使用 co_excutor async_io ACCEPT
// -----------------------------------------------------------------------

bool acceptor::accept_awaitable::await_ready() const noexcept {
    return false;
}

bool acceptor::accept_awaitable::await_suspend(std::coroutine_handle<> h) {
    peer.resize(sizeof(struct sockaddr));
    co_excutor::instance().async_io(co_excutor::CO_EVENT::ACCEPT, fd.handle(),
        peer, [this, h](int res) mutable {
            if(res >= 0) {
                result = connection(FileDescriptor(res));
            } else {
                log::erro("accept failed: {}", std::strerror(-res));
            }
            h.resume();
        });
    return true;
}

connection acceptor::accept_awaitable::await_resume() {
    return std::move(result);
}

// -----------------------------------------------------------------------
// connect_awaitable: 使用 co_excutor async_io CONNECT
// -----------------------------------------------------------------------

bool connect_awaitable::await_ready() const noexcept {
    return false;
}

bool connect_awaitable::await_suspend(std::coroutine_handle<> h) {
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if(fd < 0) {
        log::erro("socket failed: {}", std::strerror(errno));
        return false;
    }

    conn_fd = FileDescriptor(fd);

    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(static_cast<uint16_t>(port));
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    peer.resize(sizeof(addr));
    std::memcpy(peer.data(), &addr, sizeof(addr));

    // Initiate non-blocking connect before registering with asio
    int rc =
        ::connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
    if(rc == 0) {
        // 连接完成，不挂起
        result = connection(conn_fd);
        return false;
    }
    if(errno != EINPROGRESS) {
        log::erro("connect failed: {}", std::strerror(errno));
        return false;
    }

    // Wait for connection completion
    co_excutor::instance().async_io(
        co_excutor::CO_EVENT::CONNECT, fd, peer, [this, h](int res) mutable {
            if(res == 0) {
                result = connection(conn_fd);
            } else {
                log::erro("connect failed: {}", std::strerror(-res));
            }
            h.resume();
        });
    return true;
}

connection connect_awaitable::await_resume() {
    return std::move(result);
}

// -----------------------------------------------------------------------

acceptor co_listen(int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if(fd < 0) {
        log::erro("socket failed: {}", std::strerror(errno));
        return acceptor{};
    }

    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(port));

    if(::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) <
        0) {
        log::erro("bind failed: {}", std::strerror(errno));
        ::close(fd);
        return acceptor{};
    }

    if(::listen(fd, 128) < 0) {
        log::erro("listen failed: {}", std::strerror(errno));
        ::close(fd);
        return acceptor{};
    }

    log::info("Listening on port {}", port);
    return acceptor{FileDescriptor(fd)};
}
