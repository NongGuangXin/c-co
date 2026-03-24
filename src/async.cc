#include "async.h"

#include <vector>
#include <expected>
#include <coroutine>
#include <functional>

#include <cerrno>
#include <cstring>
#include <cstddef>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "excutor.h"
#include "fd.h"

static std::expected<int, int> create_listen_socket(int port) {
    int sock = -1;
    do {
        sock = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if(sock < 0) {
            log::erro("socket creation failed: {}", ::strerror(errno));
            break;
        }

        int opt = 1;
        if(::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) <
            0) {
            log::erro("setsockopt failed: {}", ::strerror(errno));
            break;
        }

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(port);
        if(::bind(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
            log::erro("bind failed: {}", ::strerror(errno));
            break;
        }

        if(::listen(sock, 128) < 0) {
            log::erro("listen failed: {}", ::strerror(errno));
            break;
        }
        return {sock};
    } while(0);

    std::expected<int, int> fd = std::unexpected(errno);
    if(sock > 0) { ::close(sock); }
    return fd;
}

bool set_nonblocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if(flags < 0) {
        log::erro("fcntl failed:{}", ::strerror(errno));
        return false;
    }
    return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) >= 0;
}

// acceptor

acceptor co_listen(int port) {
    std::expected<int, int> fd = create_listen_socket(port);
    if(fd.has_value()) { return acceptor{FileDescriptor{fd.value()}}; }
    return acceptor{FileDescriptor{-fd.error()}};
}

acceptor::operator bool() const noexcept {
    if(fd_ && fd_.handle() > 0) return true;
    return false;
}

struct acceptor::accept_awaitable {
    acceptor&      ac_;
    FileDescriptor conn;

    accept_awaitable(acceptor& ac): ac_(ac) { }

    bool await_ready() noexcept {
        conn = ::accept(ac_.handle(), nullptr, nullptr);
        if(conn) { return true; }

        if(errno == EAGAIN || errno == EWOULDBLOCK) { return false; }

        return true;
    }

    void await_suspend(std::coroutine_handle<> h) {
        auto accept_cb = [this, h]() {
            int fd = ::accept(ac_.handle(), nullptr, nullptr);
            if(fd > 0) {
                set_nonblocking(fd);
                conn = fd;
            } else {
                conn = -errno;
            }
            h.resume();
            return;
        };

        excutor::instance().suspend(ac_.fd_, co_event::READ, accept_cb);
    }

    FileDescriptor await_resume() {
        return conn;
    }
};

int acceptor::handle() {
    return fd_.handle();
}

task<connection> acceptor::co_accept() {
    co_return co_await accept_awaitable{*this};
}

// connection

int connection::handle() {
    return fd_.handle();
}

connection::operator bool() const noexcept {
    if(fd_ && fd_.handle() > 0) return true;
    return false;
}

struct connection::read_awaitable {
    connection&                 conn_;
    std::vector<unsigned char>& buf_;
    std::expected<size_t, int>  result;

    read_awaitable(connection& c, std::vector<unsigned char>& buf):
        conn_(c), buf_(buf) {
        if(buf_.capacity() < 1024) { buf_.reserve(1024); }
    }

    bool await_ready() {
        return __read_data();
    }

    void await_suspend(std::coroutine_handle<> h) {
        task_t cb_task = [this, h]() {
            log::dbug("connection read callback call");

            __read_data();
            h.resume();
            return;
        };

        excutor::instance().suspend(conn_.fd_, co_event::READ, cb_task);
        return;
    }

    std::expected<size_t, int> await_resume() {
        return result;
    }

    bool __read_data() {
        size_t bufsize = buf_.size();
        size_t remaind = buf_.size();
        size_t pos     = 0;
        do {
            ssize_t n = ::read(conn_.handle(), buf_.data() + pos, remaind);
            if(n < 0 && pos <= 0) {
                if(errno == EAGAIN || errno == EWOULDBLOCK) {
                    return false; // 无数据，挂起
                }
                result = std::unexpected(errno);
                log::erro(
                    "read error:{}:{}, not suspend", errno, ::strerror(errno));
                return true; // 错误，无需挂起
            }

            if(static_cast<size_t>(n) < remaind) {
                result = static_cast<size_t>(pos + n);
                return true; // 读完了，无需挂起
            }
            pos += n;

            // 扩容
            buf_.reserve(bufsize * 2);
            bufsize = buf_.size();
            remaind = bufsize - pos;
        } while(true);
    }
};

struct connection::write_awaitable {
    connection&                       conn_;
    std::expected<size_t, int>        result;
    const std::vector<unsigned char>& buf_;

    size_t total_written = 0;
    size_t total_size;

    write_awaitable(connection& c, const std::vector<unsigned char>& buf):
        conn_(c), buf_(buf) {
        total_size = buf.size();
    }

    bool await_ready() { // false == 挂起，true == 恢复
        while(total_written < total_size) {
            ssize_t n = ::write(conn_.handle(), buf_.data() + total_written,
                total_size - total_written);

            if(n < 0) {
                if(errno == EAGAIN || errno == EWOULDBLOCK) {
                    return false; // 挂起
                }
                if(errno == EINTR) { continue; }

                result = std::unexpected(errno);
                return true;
            }

            if(n == 0) { // 对端关闭
                result = n;
                return true;
            }

            total_written += static_cast<size_t>(n);
        }

        return true;
    }

    void await_suspend(std::coroutine_handle<> h) {
        task_t write_cb;

        write_cb = [this, h, &write_cb]() {
            while(total_written < total_size) {
                ssize_t n = ::write(conn_.handle(), buf_.data() + total_written,
                    total_size - total_written);

                if(n > 0) { // 写入
                    total_written += static_cast<size_t>(n);
                    if(total_written >= total_size) { // 写完恢复
                        result = total_written;
                        h.resume();
                        return;
                    }
                } else if(n == -1) {
                    if(errno == EAGAIN || errno == EWOULDBLOCK) { // 重试
                        excutor::instance().suspend(
                            conn_.fd_, co_event::WRITE, write_cb);
                        return;
                    } else if(errno == EINTR) { // 中断
                        continue;
                    } else { // 其他错误
                        result = std::unexpected(errno);
                        h.resume();
                        return;
                    }
                } else { // n == 0 对端关闭
                    result = static_cast<size_t>(n);
                    h.resume();
                    return;
                }
            }

            result = total_written;
            h.resume();
            return;
        };

        excutor::instance().suspend(conn_.fd_, co_event::WRITE, write_cb);

        return;
    }

    std::expected<size_t, int> await_resume() {
        return result;
    }
};

task<std::expected<size_t, int>> connection::co_read(
    std::vector<unsigned char>& buf) {
    co_return co_await read_awaitable{*this, buf};
}

task<std::expected<size_t, int>> connection::co_write(
    const std::vector<unsigned char>& buf) {
    co_return co_await write_awaitable{*this, buf};
}

// co_connect

task<connection> co_connect(int port) {
    FileDescriptor sock = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if(sock.handle() < 0) {
        log::erro("socket creation failed: {}", ::strerror(errno));
        co_return connection{-errno};
    }

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port        = htons(port);
    int ret = ::connect(sock.handle(), (sockaddr*)&addr, sizeof(addr));
    if(ret < 0 && errno != EINPROGRESS) {
        log::erro("connect failed: {}", ::strerror(errno));
        co_return connection{FileDescriptor{-errno}};
    }

    if(ret == 0) { co_return connection{sock}; }

    struct connect_awaitable {
        FileDescriptor fd;
        int            result;

        connect_awaitable(FileDescriptor s): fd(s) { }

        bool await_ready() noexcept {
            return false;
        }

        void await_suspend(std::coroutine_handle<> h) {
            task_t connect_cb = [this, h]() {
                int       err = 0;
                socklen_t len = sizeof(err);
                if(getsockopt(fd.handle(), SOL_SOCKET, SO_ERROR, &err, &len) <
                    0) {
                    result = -errno;
                } else if(err != 0) {
                    result = -err;
                } else {
                    result = 0;
                }
                h.resume();
            };

            excutor::instance().suspend(fd, co_event::WRITE, connect_cb);
        }

        FileDescriptor await_resume() {
            if(result < 0) { return FileDescriptor{result}; }
            return fd;
        }
    };

    co_return co_await connect_awaitable{sock};
}
