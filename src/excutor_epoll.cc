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

// ---------------------------------------------------------------------------
// excutor_epoll 实现：多 epoll 实例，按 fd 分片，并行处理就绪事件
// 优化：使用 epoll_event.data.ptr 直接携带上下文指针，消除 unordered_map
// ---------------------------------------------------------------------------

struct epoll_io_context {
    co_excutor::CO_EVENT event;
    int fd;
    void* buf;
    size_t len;
    io_callback_t cb;
};

// ---------------------------------------------------------------------------
// Per-instance lock-free object pool for epoll_io_context
// ---------------------------------------------------------------------------

class epoll_ctx_pool {
  public:
    ~epoll_ctx_pool() {
        for(auto* p: all_allocated_) delete p;
    }

    epoll_io_context* acquire(co_excutor::CO_EVENT event, int fd, void* buf,
        size_t len, io_callback_t&& cb) {
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

// ---------------------------------------------------------------------------
// 单个 epoll 实例，使用 data.ptr 直接传递上下文，无需 pending map
// ---------------------------------------------------------------------------

// Sentinel pointer to identify eventfd wakeup events
static epoll_io_context g_eventfd_sentinel;

class epoll_instance {
  public:
    explicit epoll_instance(std::atomic<bool>& running): running_(running) {
        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if(epoll_fd_ < 0) {
            log::erro("excutor_epoll: epoll_create1 failed: {}",
                std::strerror(errno));
            return;
        }

        event_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if(event_fd_ < 0) {
            log::erro(
                "excutor_epoll: eventfd failed: {}", std::strerror(errno));
            ::close(epoll_fd_);
            epoll_fd_ = -1;
            return;
        }

        struct epoll_event ev{};
        ev.events   = EPOLLIN;
        ev.data.ptr = &g_eventfd_sentinel;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event_fd_, &ev);

        initialized_ = true;
    }

    ~epoll_instance() {
        if(event_fd_ >= 0) { ::close(event_fd_); }
        if(epoll_fd_ >= 0) { ::close(epoll_fd_); }
    }

    bool initialized() const {
        return initialized_;
    }

    void async_io(co_excutor::CO_EVENT event, int fd, void* buf, size_t len,
        io_callback_t&& cb) {
        if(!initialized_ || !running_.load(std::memory_order_relaxed)) {
            return;
        }

        auto* ctx = pool_.acquire(event, fd, buf, len, std::move(cb));

        uint32_t epoll_events = EPOLLONESHOT | EPOLLET;
        switch(event) {
        case co_excutor::CO_EVENT::READ:
        case co_excutor::CO_EVENT::ACCEPT:
            epoll_events |= EPOLLIN;
            break;
        case co_excutor::CO_EVENT::WRITE:
        case co_excutor::CO_EVENT::CONNECT:
            epoll_events |= EPOLLOUT;
            break;
        }

        struct epoll_event ev{};
        ev.events   = epoll_events;
        ev.data.ptr = ctx;

        if(::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
            if(errno == ENOENT) {
                if(::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
                    log::erro("excutor_epoll: epoll_ctl ADD failed fd={}: {}",
                        fd, std::strerror(errno));
                    ctx->cb(-errno);
                    pool_.release(ctx);
                    return;
                }
            } else {
                log::erro("excutor_epoll: epoll_ctl MOD failed fd={}: {}", fd,
                    std::strerror(errno));
                ctx->cb(-errno);
                pool_.release(ctx);
                return;
            }
        }
    }

    void wakeup() {
        if(event_fd_ >= 0) {
            uint64_t val = 1;
            ssize_t n    = ::write(event_fd_, &val, sizeof(val));
            (void)n;
        }
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

                if(ctx == &g_eventfd_sentinel) {
                    uint64_t val;
                    ssize_t n = ::read(event_fd_, &val, sizeof(val));
                    (void)n;
                    continue;
                }

                if(!ctx) continue;

                // If shutting down, don't invoke callbacks - just free context
                if(!running_.load(std::memory_order_relaxed)) {
                    pool_.release(ctx);
                    continue;
                }

                int res = 0;
                switch(ctx->event) {
                case co_excutor::CO_EVENT::READ:
                    res = do_read(ctx);
                    break;
                case co_excutor::CO_EVENT::WRITE:
                    res = do_write(ctx);
                    break;
                case co_excutor::CO_EVENT::ACCEPT:
                    res = do_accept(ctx);
                    break;
                case co_excutor::CO_EVENT::CONNECT:
                    res = do_connect(ctx);
                    break;
                }

                ctx->cb(res);
                pool_.release(ctx);
            }
        }
    }

  private:
    static int do_read(epoll_io_context* ctx) {
        ssize_t nread = ::recv(ctx->fd, ctx->buf, ctx->len, MSG_DONTWAIT);
        if(nread >= 0) {
            return static_cast<int>(nread);
        } else {
            return -errno;
        }
    }

    static int do_write(epoll_io_context* ctx) {
        ssize_t n =
            ::send(ctx->fd, ctx->buf, ctx->len, MSG_DONTWAIT | MSG_NOSIGNAL);
        return (n >= 0) ? static_cast<int>(n) : -errno;
    }

    static void optimize_socket(int fd) {
        int yes     = 1;
        int bufsize = 1048576; // 1MB socket buffers for better throughput
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
        ::setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &yes, sizeof(yes));
        ::setsockopt(fd, SOL_SOCKET, SO_SNDBUFFORCE, &bufsize, sizeof(bufsize));
        ::setsockopt(fd, SOL_SOCKET, SO_RCVBUFFORCE, &bufsize, sizeof(bufsize));
    }

    static int do_accept(epoll_io_context* ctx) {
        int client_fd =
            ::accept4(ctx->fd, static_cast<struct sockaddr*>(ctx->buf),
                reinterpret_cast<socklen_t*>(&ctx->len),
                SOCK_NONBLOCK | SOCK_CLOEXEC);
        if(client_fd >= 0) { optimize_socket(client_fd); }
        return (client_fd >= 0) ? client_fd : -errno;
    }

    static int do_connect(epoll_io_context* ctx) {
        int err       = 0;
        socklen_t len = sizeof(err);
        if(::getsockopt(ctx->fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
            return -errno;
        }
        return (err == 0) ? 0 : -err;
    }

    int epoll_fd_{-1};
    int event_fd_{-1};
    epoll_ctx_pool pool_;
    bool initialized_{false};
    std::atomic<bool>& running_;
};

// ---------------------------------------------------------------------------
// excutor_epoll_impl：管理多个 epoll_instance，按 fd % N 分片
// ---------------------------------------------------------------------------

class excutor_epoll_impl {
  public:
    excutor_epoll_impl() {
        size_t num_cpus = std::thread::hardware_concurrency();

        for(size_t i = 0; i < num_cpus; i++) {
            auto inst = std::make_unique<epoll_instance>(running_);
            if(!inst->initialized()) {
                log::erro(
                    "excutor_epoll: failed to create epoll instance {}", i);
                throw std::runtime_error(
                    "excutor_epoll: failed to create epoll instance");
            }
            instances_.push_back(std::move(inst));
        }

        for(size_t i = 0; i < num_cpus; i++) {
            loop_threads_.emplace_back(
                [this, i]() { instances_[i]->event_loop(); });
            bind_thread_to_cpu(loop_threads_.back(), static_cast<int>(i));
        }

        log::dbug("excutor_epoll: {} epoll event loops started", num_cpus);
    }

    ~excutor_epoll_impl() {
        running_.store(false, std::memory_order_release);
        // 唤醒所有 event_loop 以便退出
        for(auto& inst: instances_) { inst->wakeup(); }
        for(auto& t: loop_threads_) {
            if(t.joinable()) {
                if(t.get_id() == std::this_thread::get_id()) {
                    t.detach();
                } else {
                    t.join();
                }
            }
        }
    }

    void async_io(co_excutor::CO_EVENT event, int fd, void* buf, size_t len,
        io_callback_t&& cb) {
        size_t idx = static_cast<size_t>(fd) % instances_.size();
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
