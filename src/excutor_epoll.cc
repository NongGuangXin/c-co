#include "co_excutor.h"
#include "log.h"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <mutex>
#include <thread>
#include <atomic>
#include <utility>
#include <vector>

#include <sys/types.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>

struct epoll_io_context {
    CO_EVENT event;
    int fd;
    void* buf;
    size_t len;
    io_callback_t cb;
};

static void optimize_socket(int fd) {
    int yes     = 1;
    int bufsize = 1048576; // 1MB socket buffers for better throughput
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

// ---------------------------------------------------------------------------
// Per-instance lock-free object pool for epoll_io_context
// ---------------------------------------------------------------------------

class epoll_ctx_pool {
  public:
    ~epoll_ctx_pool() {
        for(auto* p: all_allocated_) delete p;
    }

    epoll_io_context* acquire(
        CO_EVENT event, int fd, void* buf, size_t len, io_callback_t&& cb) {
        epoll_io_context* ctx = nullptr;
        {
            std::lock_guard lock(mutex_);
            if(!freelist_.empty()) {
                ctx = freelist_.back();
                freelist_.pop_back();
            }
        }
        if(!ctx) {
            ctx = new epoll_io_context();
            std::lock_guard lock(mutex_);
            all_allocated_.push_back(ctx);
        }
        ctx->event = event;
        ctx->fd    = fd;
        ctx->buf   = buf;
        ctx->len   = len;
        ctx->cb    = std::move(cb);
        return ctx;
    }

    void release(epoll_io_context* ctx) {
        ctx->cb  = nullptr;
        ctx->buf = nullptr;
        std::lock_guard lock(mutex_);
        freelist_.push_back(ctx);
    }

  private:
    std::vector<epoll_io_context*> freelist_;
    std::vector<epoll_io_context*> all_allocated_;
    std::mutex mutex_;
};

class epoll_instance {
  public:
    explicit epoll_instance(std::atomic<bool>& running): running_(running) {
        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if(epoll_fd_ < 0) {
            log::erro("excutor_epoll: epoll_create1 failed: {}",
                std::strerror(errno));
            return;
        }
    }

    ~epoll_instance() {
        if(epoll_fd_ >= 0) { ::close(epoll_fd_); }
    }

    void async_io(
        CO_EVENT event, int fd, void* buf, size_t len, io_callback_t&& cb) {
        if(!running_.load(std::memory_order_relaxed)) { return; }

        auto* ctx = pool_.acquire(event, fd, buf, len, std::move(cb));

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
        if(rc < 0) { rc = ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev); }
        if(rc < 0) {
            log::erro("excutor_epoll: epoll_ctl ADD failed fd={}: {}", fd,
                std::strerror(errno));
            ctx->cb(-errno);
            pool_.release(ctx);
        }
        return;
    }

    // event_loop 由外部线程调用
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
                if(!ctx) { continue; }

                // If shutting down, don't invoke callbacks - just free context
                if(!running_.load(std::memory_order_relaxed)) {
                    pool_.release(ctx);
                    continue;
                }

                int res = do_io(ctx);
                ctx->cb(res);
                pool_.release(ctx);
            }
        }
    }

  private:
    int epoll_fd_{-1};
    epoll_ctx_pool pool_;
    std::atomic<bool>& running_;
};

// ---------------------------------------------------------------------------
// excutor_epoll_impl：管理多个 epoll_instance，按 fd % N 分片
// ---------------------------------------------------------------------------
class excutor_epoll_impl {
    constexpr static size_t num_cpus = 4;

  public:
    excutor_epoll_impl() {
        for(size_t i = 0; i < num_cpus; i++) {
            auto inst = std::make_unique<epoll_instance>(running_);
            instances_.push_back(std::move(inst));
        }

        for(size_t i = 0; i < num_cpus; i++) {
            loop_threads_.emplace_back(
                [this, i]() { instances_[i]->event_loop(); });
            // 没有性能提升
            // bind_thread_to_cpu(loop_threads_.back(), static_cast<int>(i));
        }

        log::dbug("excutor_epoll: {} epoll event loops started", num_cpus);
    }

    ~excutor_epoll_impl() {
        stop();
    }

    void stop() {
        bool expected = true;
        if(!running_.compare_exchange_strong(expected, false)) return;
        for(auto& t: loop_threads_) {
            if(t.joinable()) { t.join(); }
        }
    }

    void async_io(
        CO_EVENT event, int fd, void* buf, size_t len, io_callback_t&& cb) {
        size_t idx = static_cast<size_t>(fd) % num_cpus;
        instances_[idx]->async_io(
            event, fd, buf, len, std::forward<io_callback_t>(cb));
    }

  private:
    std::vector<std::unique_ptr<epoll_instance>> instances_;
    std::vector<std::thread> loop_threads_;
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
