#include "co_excutor.h"
#include "log.h"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <mutex>
#include <thread>
#include <atomic>
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
    std::vector<unsigned char>& buf;
    co_excutor::io_callback_t cb;
    ssize_t len; // -1: 自动扩容读完就绪数据, >0: 精确读取/写入 len 字节
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

    void async_io(co_excutor::CO_EVENT event, int fd,
        std::vector<unsigned char>& buf, co_excutor::io_callback_t cb,
        ssize_t len) {
        if(!initialized_ || !running_) { return; }

        auto* ctx = new epoll_io_context{event, fd, buf, std::move(cb), len};

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
                        auto cb_copy = std::move(ctx->cb);
                        delete ctx;
                        cb_copy(-errno);
                        return;
                    }
                } else {
                    log::erro("excutor_epoll: epoll_ctl MOD failed fd={}: {}",
                        fd, std::strerror(errno));
                    auto cb_copy = std::move(ctx->cb);
                    delete ctx;
                    cb_copy(-errno);
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
                    res = readall(ctx);
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
    static int readall(epoll_io_context* ctx) {
        static constexpr size_t kChunkSize = 8192;
        std::vector<unsigned char>& buf    = ctx->buf;
        bool auto_expand                   = (ctx->len == -1);
        size_t total                       = 0;
        int res                            = 0;

        while(true) {
            ssize_t n = ::recv(
                ctx->fd, buf.data() + total, buf.size() - total, MSG_DONTWAIT);
            if(n > 0) {
                total += static_cast<size_t>(n);
                if(total == buf.size()) {
                    if(auto_expand) {
                        buf.resize(total + kChunkSize);
                        continue;
                    } else {
                        res = static_cast<int>(total);
                        break;
                    }
                } else {
                    res = static_cast<int>(total);
                    buf.resize(total);
                    break;
                }
            } else if(n == 0) {
                // EOF：如果已有数据则先返回数据，下次 read 再返回 0
                // 如果无数据则返回 0 表示纯 EOF
                buf.resize(total);
                res = (total > 0) ? static_cast<int>(total) : 0;
                break;
            } else {
                if(errno == EAGAIN || errno == EWOULDBLOCK) {
                    res = static_cast<int>(total);
                    buf.resize(total);
                    break;
                }
                res = -errno;
                break;
            }
        }

        return res;
    }

    static int do_write(epoll_io_context* ctx) {
        if(ctx->len < 0) { ctx->len = 0; }
        ssize_t n = ::send(ctx->fd, ctx->buf.data() + ctx->len,
            ctx->buf.size() - ctx->len, MSG_DONTWAIT | MSG_NOSIGNAL);
        return (n >= 0) ? static_cast<int>(n) : -errno;
    }

    static int do_accept(epoll_io_context* ctx) {
        socklen_t len = static_cast<socklen_t>(ctx->buf.size());
        int client_fd = ::accept4(ctx->fd,
            reinterpret_cast<struct sockaddr*>(ctx->buf.data()), &len,
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

    void async_io(co_excutor::CO_EVENT event, int fd,
        std::vector<unsigned char>& buf, co_excutor::io_callback_t cb,
        ssize_t len) {
        size_t idx = static_cast<size_t>(fd) % instances_.size();
        instances_[idx]->async_io(event, fd, buf, std::move(cb), len);
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

void excutor_epoll::async_io(CO_EVENT event, int fd,
    std::vector<unsigned char>& buf, io_callback_t cb, ssize_t len) {
    epoll_impl().async_io(event, fd, buf, std::move(cb), len);
}
