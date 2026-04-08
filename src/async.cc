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
// read_awaitable: 使用 io_uring recv 读取一次可用数据
// -----------------------------------------------------------------------

bool connection::read_awaitable::try_read() {
    // 快速路径：尝试立即非阻塞读取
    size_t total = 0;
    for(;;) {
        if(buf.size() < total + kChunkSize) { buf.resize(total + kChunkSize); }

        ssize_t n = ::recv(
            fd.handle(), buf.data() + total, buf.size() - total, MSG_DONTWAIT);

        if(n > 0) {
            total += static_cast<size_t>(n);
            continue;
        }

        if(n == 0) {
            buf.resize(total);
            result = static_cast<size_t>(0);
            return false; // EOF，数据就绪
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
        return false; // 出错
    }
}

bool connection::read_awaitable::await_ready() const noexcept {
    return false;
}

bool connection::read_awaitable::await_suspend(std::coroutine_handle<> h) {
    // 确保 buf 至少有 kChunkSize 空间用于首次 io_uring recv
    if(buf.size() < kChunkSize) { buf.resize(kChunkSize); }

    excutor::instance().async_recv(
        fd.handle(), buf.data(), buf.size(), 0, [this, h](int res) mutable {
            if(res > 0) {
                size_t total = static_cast<size_t>(res);
                if(total == buf.size()) {
                    // io_uring 唤醒后, 如果buf读满，继续非阻塞 drain 所有可用数据
                    for(;;) {
                        if(buf.size() < total + kChunkSize) {
                            buf.resize(total + kChunkSize);
                        }
                        ssize_t n = ::recv(fd.handle(), buf.data() + total,
                            buf.size() - total, MSG_DONTWAIT);
                        if(n > 0) {
                            total += static_cast<size_t>(n);
                            continue;
                        }
                        break; // EAGAIN / EOF / error — 停止 drain
                    }
                }
                buf.resize(total);
                result = total;
            } else if(res == 0) {
                buf.resize(0);
                result = static_cast<size_t>(0);
            } else {
                buf.resize(0);
                result = std::unexpected(-res);
            }
            h.resume();
        });
    return true;
}

std::expected<size_t, int> connection::read_awaitable::await_resume() {
    return result;
}

// -----------------------------------------------------------------------
// read_until_awaitable: 持续读取直到填满 target 字节
// -----------------------------------------------------------------------

bool connection::read_until_awaitable::try_read() {
    for(;;) {
        ssize_t n = ::recv(
            fd.handle(), buf.data() + total, target - total, MSG_DONTWAIT);

        if(n > 0) {
            total += static_cast<size_t>(n);
            if(total >= target) {
                result = total;
                return false; // 已读满
            }
            continue;
        }

        if(n == 0) {
            buf.resize(total);
            result = static_cast<size_t>(0);
            return false;
        }

        // n < 0
        if(errno == EAGAIN || errno == EWOULDBLOCK) {
            return true; // 需要挂起
        }

        buf.resize(total);
        result = std::unexpected(errno);
        return false;
    }
}

void connection::read_until_awaitable::do_read(std::coroutine_handle<> h) {
    excutor::instance().async_recv(fd.handle(), buf.data() + total,
        target - total, 0, [this, h](int res) mutable {
            if(res > 0) {
                total += static_cast<size_t>(res);
                if(total >= target) {
                    result = total;
                    h.resume();
                    return;
                }
                // 未读满，继续提交
                do_read(h);
                return;
            }

            if(res == 0) {
                buf.resize(total);
                result = static_cast<size_t>(0);
                h.resume();
                return;
            }

            // 错误
            buf.resize(total);
            result = std::unexpected(-res);
            h.resume();
        });
}

bool connection::read_until_awaitable::await_ready() const noexcept {
    return false;
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
// write_awaitable: 使用 io_uring send 写入全部数据（处理短写）
// -----------------------------------------------------------------------

bool connection::write_awaitable::try_write() {
    while(written < buf.size()) {
        ssize_t n = ::send(fd.handle(), buf.data() + written,
            buf.size() - written, MSG_DONTWAIT | MSG_NOSIGNAL);
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
    return false; // 全部写完
}

void connection::write_awaitable::do_write(std::coroutine_handle<> h) {
    excutor::instance().async_send(fd.handle(), buf.data() + written,
        buf.size() - written, 0, [this, h](int res) mutable {
            if(res < 0) {
                result = std::unexpected(-res);
                h.resume();
                return;
            }
            written += static_cast<size_t>(res);
            if(written < buf.size()) {
                do_write(h); // 短写，继续
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
    do_write(h);
    return true;
}

std::expected<size_t, int> connection::write_awaitable::await_resume() {
    return result;
}

// -----------------------------------------------------------------------
// accept_awaitable: 使用 io_uring accept
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
    excutor::instance().async_accept(fd.handle(), nullptr, nullptr,
        SOCK_NONBLOCK | SOCK_CLOEXEC, [this, h](int res) mutable {
            if(res >= 0) {
                result = connection(FileDescriptor(res));
            } else {
                log::erro("io_uring accept failed: {}", std::strerror(-res));
            }
            h.resume();
        });
    return true;
}

connection acceptor::accept_awaitable::await_resume() {
    return std::move(result);
}

// -----------------------------------------------------------------------
// connect_awaitable: 使用 io_uring connect
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

    excutor::instance().async_connect(fd,
        reinterpret_cast<const sockaddr*>(&addr), sizeof(addr),
        [this, h](int res) mutable {
            if(res == 0) {
                result = connection(conn_fd);
            } else {
                log::erro("io_uring connect failed: {}", std::strerror(-res));
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
