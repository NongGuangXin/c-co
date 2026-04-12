#include "async.h"
#include "co_excutor.h"
#include "log.h"

#include <cerrno>
#include <cstddef>
#include <cstring>

#include <expected>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <utility>
#include <vector>

// -----------------------------------------------------------------------
// read_awaitable: 使用 co_excutor async_io READ 读取一次可用数据
// -----------------------------------------------------------------------

bool connection::read_awaitable::await_ready() noexcept {
    // 尝试读取，读到的话就不用挂起了
    ssize_t n = ::recv(fd.handle(), buf.data(), buf.size(), MSG_DONTWAIT);
    if(n >= 0) {
        result = static_cast<size_t>(n);
        return true;
    } else if(errno == EAGAIN || errno == EWOULDBLOCK) {
        return false;
    }

    result = std::unexpected(errno);
    return true;
}

bool connection::read_awaitable::await_suspend(std::coroutine_handle<> h) {
    io_callback_t read_cb = [this, h](int res) mutable {
        if(res >= 0) {
            result = static_cast<size_t>(res);
        } else {
            result = std::unexpected(-res);
        }
        h.resume();
        return;
    };

    co_excutor::instance().async_io(co_excutor::CO_EVENT::READ, fd.handle(),
        buf.data(), buf.size(), std::forward<io_callback_t>(read_cb));
    return true;
}

std::expected<size_t, int> connection::read_awaitable::await_resume() {
    return result;
}

// -----------------------------------------------------------------------
// read_until_awaitable: 持续读取直到填满buf
// -----------------------------------------------------------------------

bool connection::read_until_awaitable::await_ready() const noexcept {
    return false;
}

void connection::read_until_awaitable::do_read(std::coroutine_handle<> h) {
    io_callback_t read_cb = [this, h](int res) mutable {
        if(res < 0) {
            result = std::unexpected(-res);
            h.resume();
            return;
        }

        if(res == 0) {
            // EOF — 对端关闭，返回已读取的字节数
            result = total;
            h.resume();
            return;
        }

        total += static_cast<size_t>(res);
        if(total >= target) {
            result = total;
            h.resume();
            return;
        }

        do_read(h);
    };

    void* p    = buf.data() + total;
    size_t len = target - total;

    co_excutor::instance().async_io(co_excutor::CO_EVENT::READ, fd.handle(), p,
        len, std::forward<io_callback_t>(read_cb));
}

bool connection::read_until_awaitable::await_suspend(
    std::coroutine_handle<> h) {
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
    io_callback_t write_cb = [this, h](int res) mutable {
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

    void* p = const_cast<unsigned char*>(buf.data()) + written;

    co_excutor::instance().async_io(co_excutor::CO_EVENT::WRITE, fd.handle(), p,
        buf.size() - written, std::move(write_cb));
}

bool connection::write_awaitable::await_ready() noexcept {
    ssize_t nsend = 0;
    while(written < buf.size()) {
        nsend = ::send(fd.handle(), buf.data() + written, buf.size() - written,
            MSG_DONTWAIT | MSG_NOSIGNAL);
        if(nsend > 0) {
            written += static_cast<size_t>(nsend);
            continue;
        }
        break;
    }

    // 写完
    if(written >= buf.size()) {
        result = written;
        return true;
    }

    if(nsend >= 0 || errno == EAGAIN || errno == EWOULDBLOCK) {
        return false;
    }

    result = std::unexpected(errno);
    return true;
}

bool connection::write_awaitable::await_suspend(std::coroutine_handle<> h) {
    do_write(h);
    return true;
}

std::expected<size_t, int> connection::write_awaitable::await_resume() {
    return result;
}

// -----------------------------------------------------------------------
// accept_awaitable: 使用 co_excutor async_io ACCEPT
// -----------------------------------------------------------------------

bool acceptor::accept_awaitable::await_ready() const noexcept {
    return false;
}

bool acceptor::accept_awaitable::await_suspend(std::coroutine_handle<> h) {
    co_excutor::instance().async_io(co_excutor::CO_EVENT::ACCEPT, fd.handle(),
        &addr, sizeof(addr), [this, h](int res) mutable {
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
    return result;
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

    // Initiate non-blocking connect before registering with asio
    int rc = ::connect(conn_fd.handle(),
        reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
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
    co_excutor::instance().async_io(co_excutor::CO_EVENT::CONNECT,
        conn_fd.handle(), static_cast<void*>(&addr), sizeof(addr),
        [this, h](int res) mutable {
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
    return result;
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
