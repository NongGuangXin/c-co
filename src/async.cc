#include "async.h"
#include "log.h"

#include <cerrno>
#include <cstring>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

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
            int       err = 0;
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
