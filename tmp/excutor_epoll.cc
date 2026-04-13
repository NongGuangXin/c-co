#include "co_excutor.h"
#include "log.h"

#include <cerrno>
#include <cstring>
#include <thread>
#include <atomic>
#include <vector>

#include <sys/epoll.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

// ---------------------------------------------------------------------------
// excutor_epoll 实现：单 epoll 实例，简化实现
// ---------------------------------------------------------------------------

struct epoll_io_context {
    CO_EVENT event;
    int fd;
    void* buf;
    size_t len;
    io_callback_t cb;
};

static void optimize_socket(int fd) {
    int yes     = 1;
    int bufsize = 1048576;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    ::setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &yes, sizeof(yes));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDBUFFORCE, &bufsize, sizeof(bufsize));
    ::setsockopt(fd, SOL_SOCKET, SO_RCVBUFFORCE, &bufsize, sizeof(bufsize));
}

static int do_io(epoll_io_context* ctx) {
    switch(ctx->event) {
    case CO_EVENT::RECV: {
        ssize_t n = ::recv(ctx->fd, ctx->buf, ctx->len, MSG_DONTWAIT);
        return (n >= 0) ? static_cast<int>(n) : -errno;
    }
    case CO_EVENT::SEND: {
        ssize_t n =
            ::send(ctx->fd, ctx->buf, ctx->len, MSG_DONTWAIT | MSG_NOSIGNAL);
        return (n >= 0) ? static_cast<int>(n) : -errno;
    }
    case CO_EVENT::ACCEPT: {
        int client_fd =
            ::accept4(ctx->fd, static_cast<struct sockaddr*>(ctx->buf),
                reinterpret_cast<socklen_t*>(&ctx->len),
                SOCK_NONBLOCK | SOCK_CLOEXEC);
        if(client_fd >= 0) { optimize_socket(client_fd); }
        return (client_fd >= 0) ? client_fd : -errno;
    }
    case CO_EVENT::CONNECT: {
        int err       = 0;
        socklen_t len = sizeof(err);
        if(::getsockopt(ctx->fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
            return -errno;
        }
        return (err == 0) ? 0 : -err;
    }
    }
    return -EINVAL;
}

class excutor_epoll_impl {
  public:
    excutor_epoll_impl() {
        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if(epoll_fd_ < 0) throw std::runtime_error("epoll_create1 failed");

        size_t num_cpus = std::thread::hardware_concurrency();
        for(size_t i = 0; i < num_cpus; i++) {
            threads_.emplace_back([this]() { event_loop(); });
            bind_thread_to_cpu(threads_.back(), static_cast<int>(i));
        }
        log::dbug("excutor_epoll: {} epoll event loops started", num_cpus);
    }

    ~excutor_epoll_impl() {
        stop();
    }

    void stop() {
        bool expected = true;
        if(!running_.compare_exchange_strong(expected, false)) return;
        for(auto& t: threads_) {
            if(t.joinable()) {
                if(t.get_id() == std::this_thread::get_id()) {
                    t.detach();
                } else {
                    t.join();
                }
            }
        }
        if(epoll_fd_ >= 0) { ::close(epoll_fd_); }
    }

    void async_io(
        CO_EVENT event, int fd, void* buf, size_t len, io_callback_t&& cb) {
        if(!running_.load(std::memory_order_relaxed)) return;

        auto* ctx = new epoll_io_context{event, fd, buf, len, std::move(cb)};

        uint32_t epoll_events = EPOLLONESHOT | EPOLLET;
        switch(event) {
        case CO_EVENT::RECV:
        case CO_EVENT::ACCEPT:
            epoll_events |= EPOLLIN;
            break;
        case CO_EVENT::SEND:
        case CO_EVENT::CONNECT:
            epoll_events |= EPOLLOUT;
            break;
        }

        struct epoll_event ev{};
        ev.events   = epoll_events;
        ev.data.ptr = ctx;

        int rc = ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
        if(rc < 0 && errno == ENOENT) {
            rc = ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
        }
        if(rc < 0) {
            log::erro("excutor_epoll: epoll_ctl failed fd={}: {}", fd,
                std::strerror(errno));
            ctx->cb(-errno);
            delete ctx;
        }
        return;
    }

  private:
    void event_loop() {
        static constexpr int MAX_EVENTS = 1024;
        struct epoll_event events[MAX_EVENTS];

        while(running_.load(std::memory_order_relaxed)) {
            int nfds = ::epoll_wait(epoll_fd_, events, MAX_EVENTS, 5);
            if(nfds < 0) {
                if(errno == EINTR) continue;
                log::erro("excutor_epoll: epoll_wait error: {}",
                    std::strerror(errno));
                break;
            }

            for(int i = 0; i < nfds; i++) {
                auto* ctx = static_cast<epoll_io_context*>(events[i].data.ptr);
                if(!ctx) continue;

                if(!running_.load(std::memory_order_relaxed)) {
                    delete ctx;
                    continue;
                }

                ctx->cb(do_io(ctx));
                delete ctx;
            }
        }
    }

    int epoll_fd_{-1};
    std::vector<std::thread> threads_;
    std::atomic<bool> running_{true};
};

static excutor_epoll_impl& epoll_impl() {
    static excutor_epoll_impl instance;
    return instance;
}

void excutor_epoll::async_io(
    CO_EVENT event, int fd, void* buf, size_t len, io_callback_t&& cb) {
    epoll_impl().async_io(event, fd, buf, len, std::forward<io_callback_t>(cb));
}

void excutor_epoll::stop() {
    epoll_impl().stop();
}
