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
#include <functional>
#include <unordered_map>

#include <sys/types.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

// ---------------------------------------------------------------------------
// excutor_epoll 实现：多 epoll 实例，按 fd 分片，并行处理就绪事件
// ---------------------------------------------------------------------------

struct epoll_io_context {
    co_excutor::CO_EVENT event;
    int fd;
    void* buf;
    size_t len;
    io_callback_t cb;
};

// ---------------------------------------------------------------------------
// 单个 epoll 实例，拥有独立的 epoll fd、eventfd、event_loop 线程和 pending map
// ---------------------------------------------------------------------------

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
        ev.events  = EPOLLIN;
        ev.data.fd = event_fd_;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event_fd_, &ev);

        initialized_ = true;
    }

    ~epoll_instance() {
        if(event_fd_ >= 0) { ::close(event_fd_); }
        if(epoll_fd_ >= 0) {
            // 清理残留 pending（不回调，进程正在退出）
            for(auto& [fd, ctx]: pending_) {
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                delete ctx;
            }
            pending_.clear();
            ::close(epoll_fd_);
        }
    }

    bool initialized() const {
        return initialized_;
    }

    void async_io(co_excutor::CO_EVENT event, int fd, void* buf, size_t len,
        io_callback_t&& cb) {
        if(!initialized_ || !running_) { return; }

        auto* ctx = new epoll_io_context{event, fd, buf, len, cb};

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

        {
            std::lock_guard lock(mutex_);

            struct epoll_event ev{};
            ev.events  = epoll_events;
            ev.data.fd = fd;

            if(::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
                if(errno == ENOENT) {
                    if(::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
                        log::erro(
                            "excutor_epoll: epoll_ctl ADD failed fd={}: {}", fd,
                            std::strerror(errno));
                        ctx->cb(-errno);
                        delete ctx;
                        return;
                    }
                } else {
                    log::erro("excutor_epoll: epoll_ctl MOD failed fd={}: {}",
                        fd, std::strerror(errno));

                    ctx->cb(-errno);
                    delete ctx;
                    return;
                }
            }

            pending_[fd] = ctx;
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
        static constexpr int MAX_EVENTS = 64;
        struct epoll_event events[MAX_EVENTS];

        while(running_) {
            int nfds = ::epoll_wait(epoll_fd_, events, MAX_EVENTS, 100);

            if(nfds < 0) {
                if(errno == EINTR) continue;
                log::erro("excutor_epoll: epoll_wait error: {}",
                    std::strerror(errno));
                break;
            }

            for(int i = 0; i < nfds; i++) {
                int fd = events[i].data.fd;

                if(fd == event_fd_) {
                    uint64_t val;
                    ssize_t n = ::read(event_fd_, &val, sizeof(val));
                    (void)n;
                    continue;
                }

                epoll_io_context* ctx = nullptr;
                {
                    std::lock_guard lock(mutex_);
                    auto it = pending_.find(fd);
                    if(it == pending_.end()) continue;
                    ctx = it->second;
                    pending_.erase(it);
                }

                if(!ctx) continue;

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
                delete ctx;
            }
        }
    }

  private:
    static int do_read(epoll_io_context* ctx) {
        ssize_t nread = ::recv(ctx->fd, ctx->buf, ctx->len, MSG_DONTWAIT);
        if(nread >= 0) {
            return static_cast<int>(nread);
        } else {
            if(errno == EAGAIN || errno == EWOULDBLOCK) { return 0; }
            return -errno;
        }
    }

    static int do_write(epoll_io_context* ctx) {
        ssize_t n =
            ::send(ctx->fd, ctx->buf, ctx->len, MSG_DONTWAIT | MSG_NOSIGNAL);
        return (n >= 0) ? static_cast<int>(n) : -errno;
    }

    static int do_accept(epoll_io_context* ctx) {
        int client_fd =
            ::accept4(ctx->fd, static_cast<struct sockaddr*>(ctx->buf),
                reinterpret_cast<socklen_t*>(&ctx->len),
                SOCK_NONBLOCK | SOCK_CLOEXEC);
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
    std::mutex mutex_;
    std::unordered_map<int, epoll_io_context*> pending_;
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
