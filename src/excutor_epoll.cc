#include "co_excutor.h"
#include "log.h"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <sys/types.h>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <vector>

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

// ---------------------------------------------------------------------------
// excutor_epoll 实现：使用 epoll 监听 fd 就绪事件
// 根据 CO_EVENT 类型在就绪后执行 recv / send / accept4 / connect
// ---------------------------------------------------------------------------

struct epoll_io_context {
    co_excutor::CO_EVENT event;
    int fd;
    std::vector<unsigned char>& buf;
    co_excutor::io_callback_t cb;
    ssize_t len; // -1: 自动扩容读完就绪数据, >0: 精确读取/写入 len 字节
};

class excutor_epoll_impl {
  public:
    excutor_epoll_impl() {
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
        loop_thread_ = std::thread([this]() { event_loop(); });
        log::dbug("excutor_epoll: created epoll event loop");
    }

    ~excutor_epoll_impl() {
        running_ = false;

        if(event_fd_ >= 0) {
            uint64_t val = 1;
            ssize_t n    = ::write(event_fd_, &val, sizeof(val));
            (void)n;
        }

        if(loop_thread_.joinable()) { loop_thread_.join(); }

        {
            std::lock_guard lock(mutex_);
            for(auto& [fd, ctx]: pending_) {
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                delete ctx;
            }
            pending_.clear();
        }

        if(event_fd_ >= 0) { ::close(event_fd_); }
        if(epoll_fd_ >= 0) { ::close(epoll_fd_); }
    }

    void async_io(co_excutor::CO_EVENT event, int fd,
        std::vector<unsigned char>& buf, co_excutor::io_callback_t cb,
        ssize_t len = -1) {
        if(!initialized_) {
            cb(-ENOMEM);
            return;
        }

        auto* ctx = new epoll_io_context{event, fd, buf, std::move(cb), len};

        // 根据事件类型决定监听可读还是可写
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

        uint64_t val = 1;
        ssize_t n    = ::write(event_fd_, &val, sizeof(val));
        (void)n;
    }

    int __readall(epoll_io_context* ctx) {
        static constexpr size_t kChunkSize = 8192;
        std::vector<unsigned char>& buf    = ctx->buf;
        bool auto_expand                   = (ctx->len == -1);
        size_t total                       = 0;
        int res                            = 0;

        while(true) {
            ssize_t n = ::recv(
                ctx->fd, buf.data() + total, buf.size() - total, MSG_DONTWAIT);
            if(n > 0) {
                total += n;
                if(total == buf.size()) {
                    if(auto_expand) {
                        // len==-1: 自动扩容继续读
                        buf.resize(total + kChunkSize);
                        continue;
                    } else {
                        // len>0: 读满目标量即停止，多余数据留在内核缓冲区
                        res = static_cast<int>(total);
                        break;
                    }
                } else {
                    // recv 返回不足，说明暂无更多数据
                    res = static_cast<int>(total);
                    buf.resize(total);
                    break;
                }
            } else if(n == 0) {
                res = 0;
                if(total > 0) { buf.resize(total); }
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

    int __write(epoll_io_context* ctx) {
        ssize_t n = ::send(ctx->fd, ctx->buf.data() + ctx->len,
            ctx->buf.size() - ctx->len, MSG_DONTWAIT | MSG_NOSIGNAL);
        return (n >= 0) ? static_cast<int>(n) : -errno;
    }

    int __accept(epoll_io_context* ctx) {
        socklen_t len = static_cast<socklen_t>(ctx->buf.size());

        int client_fd = ::accept4(ctx->fd,
            reinterpret_cast<struct sockaddr*>(ctx->buf.data()), &len,
            SOCK_NONBLOCK | SOCK_CLOEXEC);
        return (client_fd >= 0) ? client_fd : -errno;
    }

    int __connect(epoll_io_context* ctx) {
        // EPOLLOUT fired after non-blocking connect() was initiated,
        // use getsockopt to check the result
        int err       = 0;
        socklen_t len = sizeof(err);
        if(::getsockopt(ctx->fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
            return -errno;
        }
        return (err == 0) ? 0 : -err;
    }

  private:
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
                case co_excutor::CO_EVENT::READ: {
                    res = __readall(ctx);
                    break;
                }
                case co_excutor::CO_EVENT::WRITE: {
                    res = __write(ctx);
                    break;
                }
                case co_excutor::CO_EVENT::ACCEPT: {
                    res = __accept(ctx);
                    break;
                }

                case co_excutor::CO_EVENT::CONNECT: {
                    res = __connect(ctx);
                    break;
                }
                }

                auto&& io_cb = std::move(ctx->cb);
                io_cb(res);
                delete ctx;
            }
        }
    }

    int epoll_fd_{-1};
    int event_fd_{-1};
    std::mutex mutex_;
    std::unordered_map<int, epoll_io_context*> pending_;
    std::thread loop_thread_;
    std::atomic<bool> running_{true};
    bool initialized_{false};
};

static excutor_epoll_impl& epoll_impl() {
    static excutor_epoll_impl instance;
    return instance;
}

void excutor_epoll::async_io(CO_EVENT event, int fd,
    std::vector<unsigned char>& buf, io_callback_t cb, ssize_t len) {
    epoll_impl().async_io(event, fd, buf, std::move(cb), len);
}
