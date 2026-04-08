#include "async.h"
#include "log.h"

#include <cerrno>
#include <cstring>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

// -----------------------------------------------------------------------

// false - 就绪
bool connection::read_awaitable::try_read() {
    for(;;) {
        if(buf.size() < total + kChunkSize) { buf.resize(total + kChunkSize); }

        ssize_t n = ::read(fd.handle(), buf.data() + total, buf.size() - total);

        if(n > 0) {
            total += static_cast<size_t>(n);
            continue;
        }

        if(n == 0) {
            buf.resize(total);
            result = n;
            return false; // 连接关闭，数据就绪
        }

        // n < 0
        if(errno == EAGAIN || errno == EWOULDBLOCK) {
            if(total > 0) {
                buf.resize(total);
                result = total;
                return false; // 已读到数据，无需挂起
            }
            return true; // 无数据，需要挂起
        }

        buf.resize(total);
        result = std::unexpected(errno);
        return false; // 出错，数据就绪（含错误）
    }
}

void connection::read_awaitable::do_read(std::coroutine_handle<> h) {
    auto read_cb = [this, h]() mutable {
        for(;;) {
            if(buf.size() < total + kChunkSize) {
                buf.resize(total + kChunkSize);
            }

            ssize_t n =
                ::read(fd.handle(), buf.data() + total, buf.size() - total);

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

// 返回 true-不挂起，false-挂起等待
bool connection::read_awaitable::await_ready() const noexcept {
    return false;
}

// 返回 true-挂起，false-不挂起
bool connection::read_awaitable::await_suspend(std::coroutine_handle<> h) {
    total = 0;
    if(try_read() == false) { return false; }
    do_read(h);
    return true;
}

std::expected<size_t, int> connection::read_awaitable::await_resume() {
    return result;
}

// -----------------------------------

bool connection::read_until_awaitable::try_read() {
    for(;;) {
        ssize_t n = ::read(fd.handle(), buf.data() + total, target - total);

        if(n > 0) {
            total += static_cast<size_t>(n);
            if(total >= target) {
                result = total;
                return false; // 已读满，无需挂起
            }
            continue;
        }

        if(n == 0) {
            buf.resize(total);
            result = n;
            return false;
        }

        // n < 0
        if(errno == EAGAIN || errno == EWOULDBLOCK) {
            return true; // 需要挂起等更多数据
        }

        buf.resize(total);
        result = std::unexpected(errno);
        return false;
    }
}

void connection::read_until_awaitable::do_read(std::coroutine_handle<> h) {
    auto read_cb = [this, h]() mutable {
        for(;;) {
            ssize_t n = ::read(fd.handle(), buf.data() + total, target - total);

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

bool connection::read_until_awaitable::await_ready() const noexcept {
    return false;
}

bool connection::read_until_awaitable::await_suspend(
    std::coroutine_handle<> h) {
    target = buf.size();
    total  = 0;
    if(try_read() == false) { return false; }
    do_read(h);
    return true;
}

std::expected<size_t, int> connection::read_until_awaitable::await_resume() {
    return result;
}

// -----------------------------------

bool connection::write_awaitable::try_write() {
    while(written < buf.size()) {
        ssize_t n =
            ::write(fd.handle(), buf.data() + written, buf.size() - written);
        if(n < 0) {
            if(errno == EAGAIN || errno == EWOULDBLOCK) {
                return true; // 写缓冲区满，需要挂起
            }
            result = std::unexpected(errno);
            return false;
        }
        written += static_cast<size_t>(n);
    }
    result = written;
    return false; // 全部写完，无需挂起
}

void connection::write_awaitable::do_write(std::coroutine_handle<> h) {
    excutor::instance().register_event(fd, excutor::WRITE, [this, h]() mutable {
        ssize_t n =
            ::write(fd.handle(), buf.data() + written, buf.size() - written);
        if(n < 0) {
            result = std::unexpected(errno);
            h.resume();
            return;
        }
        written += static_cast<size_t>(n);
        if(written < buf.size()) {
            do_write(h);
        } else {
            result = written;
            h.resume();
        }
    });
}

bool connection::write_awaitable::await_ready() const noexcept {
    return false;
}

bool connection::write_awaitable::await_suspend(std::coroutine_handle<> h) {
    if(try_write() == false) { return false; }
    do_write(h);
    return true;
}

std::expected<size_t, int> connection::write_awaitable::await_resume() {
    return result;
}

// -----------------------------------------------------------------------

bool acceptor::accept_awaitable::try_accept() {
    int client_fd =
        ::accept4(fd.handle(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if(client_fd >= 0) {
        result = connection(FileDescriptor(client_fd));
        return false; // 不挂起
    }
    if(errno != EAGAIN && errno != EWOULDBLOCK) {
        log::erro("accept4 failed: {}", std::strerror(errno));
        return false;
    }
    return true;
}

bool acceptor::accept_awaitable::await_ready() const noexcept {
    return false;
}

bool acceptor::accept_awaitable::await_suspend(std::coroutine_handle<> h) {
    // 先尝试立即 accept
    if(try_accept() == false) { return false; }

    auto accept_cb = [this, h]() mutable {
        int client_fd2 = ::accept4(
            fd.handle(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if(client_fd2 < 0) {
            log::erro("accept4 failed: {}", std::strerror(errno));
        } else {
            result = connection(FileDescriptor(client_fd2));
        }
        h.resume();
    };

    // 无连接可接受，挂起等待
    excutor::instance().register_event(fd, excutor::READ, accept_cb);
    return true;
}

connection acceptor::accept_awaitable::await_resume() {
    return std::move(result);
}

// ------------------------------------------------------------------------

bool connect_awaitable::await_ready() const noexcept {
    return false;
}

bool connect_awaitable::await_suspend(std::coroutine_handle<> h) {
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if(fd < 0) {
        log::erro("socket failed: {}", std::strerror(errno));
        // 返回 false: 不挂起，立即 resume（result 为空 connection）
        return false;
    }

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(static_cast<uint16_t>(port));
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    int ret =
        ::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    if(ret == 0) {
        // Connected immediately - 不挂起
        result = connection(FileDescriptor(fd));
        return false;
    }

    if(errno != EINPROGRESS) {
        log::erro("connect failed: {}", std::strerror(errno));
        ::close(fd);
        return false;
    }

    // Connection in progress, 挂起等待 write-ready
    auto conn_fd = FileDescriptor(fd);
    excutor::instance().register_event(
        conn_fd, excutor::WRITE, [this, h, conn_fd]() mutable {
            int err       = 0;
            socklen_t len = sizeof(err);
            ::getsockopt(conn_fd.handle(), SOL_SOCKET, SO_ERROR, &err, &len);
            if(err == 0) {
                result = connection(conn_fd);
            } else {
                log::erro("connect error: {}", std::strerror(err));
            }
            h.resume();
        });
    return true; // 挂起
}

connection connect_awaitable::await_resume() {
    return std::move(result);
}

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
