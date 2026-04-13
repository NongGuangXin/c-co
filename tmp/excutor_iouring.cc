#include "co_excutor.h"
#include "log.h"

#include <cerrno>
#include <cstring>
#include <mutex>
#include <thread>
#include <atomic>
#include <vector>

#include <liburing.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

// ---------------------------------------------------------------------------
// excutor_uring 实现：单 io_uring 实例，简化实现
// ---------------------------------------------------------------------------

struct uring_io_context {
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

static void prep_sqe(struct io_uring_sqe* sqe, uring_io_context* ctx) {
    switch(ctx->event) {
    case CO_EVENT::RECV:
        io_uring_prep_recv(sqe, ctx->fd, ctx->buf, ctx->len, 0);
        break;
    case CO_EVENT::SEND:
        io_uring_prep_send(sqe, ctx->fd, ctx->buf, ctx->len, MSG_NOSIGNAL);
        break;
    case CO_EVENT::ACCEPT:
        io_uring_prep_accept(sqe, ctx->fd,
            reinterpret_cast<struct sockaddr*>(ctx->buf),
            reinterpret_cast<socklen_t*>(&ctx->len),
            SOCK_NONBLOCK | SOCK_CLOEXEC);
        break;
    case CO_EVENT::CONNECT:
        io_uring_prep_connect(sqe, ctx->fd,
            reinterpret_cast<const sockaddr*>(ctx->buf),
            static_cast<socklen_t>(ctx->len));
        break;
    }
}

class excutor_uring_impl {
  public:
    excutor_uring_impl() {
        struct io_uring_params params{};
        int rc = io_uring_queue_init_params(4096, &ring_, &params);
        if(rc < 0) throw std::runtime_error("io_uring_queue_init failed");

        size_t num_cpus = std::thread::hardware_concurrency();
        for(size_t i = 0; i < num_cpus; i++) {
            threads_.emplace_back([this]() { event_loop(); });
            bind_thread_to_cpu(threads_.back(), static_cast<int>(i));
        }
        log::dbug("excutor_uring: {} io_uring event loops started", num_cpus);
    }

    ~excutor_uring_impl() {
        stop();
    }

    void stop() {
        bool expected = true;
        if(!running_.compare_exchange_strong(expected, false)) return;
        for(auto& t: threads_) {
            if(t.joinable()) {
                if(t.get_id() == std::this_thread::get_id())
                    t.detach();
                else
                    t.join();
            }
        }
        io_uring_queue_exit(&ring_);
    }

    void async_io(
        CO_EVENT event, int fd, void* buf, size_t len, io_callback_t&& cb) {
        if(!running_.load(std::memory_order_relaxed)) return;

        std::lock_guard lock(sq_mutex_);
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if(!sqe) {
            io_uring_submit(&ring_);
            sqe = io_uring_get_sqe(&ring_);
            if(!sqe) {
                log::erro("excutor_uring: io_uring_get_sqe failed fd={}", fd);
                cb(-ENOMEM);
                return;
            }
        }

        auto* ctx = new uring_io_context{event, fd, buf, len, std::move(cb)};
        prep_sqe(sqe, ctx);
        io_uring_sqe_set_data(sqe, ctx);
        io_uring_submit(&ring_);
    }

  private:
    void event_loop() {
        while(running_.load(std::memory_order_relaxed)) {
            struct io_uring_cqe* cqe = nullptr;
            struct __kernel_timespec ts{};
            ts.tv_nsec = 5'000'000; // 5ms

            int ret = io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
            if(ret < 0) {
                if(ret == -ETIME || ret == -EINTR) { continue; }
                log::erro(
                    "excutor_uring: wait_cqe error: {}", std::strerror(-ret));
                break;
            }

            unsigned head;
            unsigned count = 0;
            io_uring_for_each_cqe(&ring_, head, cqe) {
                count++;
                auto* ctx =
                    static_cast<uring_io_context*>(io_uring_cqe_get_data(cqe));
                if(!ctx) { continue; }

                if(!running_.load(std::memory_order_relaxed)) {
                    delete ctx;
                    continue;
                }

                if(ctx->event == CO_EVENT::ACCEPT && cqe->res >= 0) {
                    optimize_socket(cqe->res);
                }

                ctx->cb(cqe->res);
                delete ctx;
            }
            io_uring_cq_advance(&ring_, count);
        }
    }

    struct io_uring ring_{};
    std::mutex sq_mutex_;
    std::vector<std::thread> threads_;
    std::atomic<bool> running_{true};
};

static excutor_uring_impl& uring_impl() {
    static excutor_uring_impl instance;
    return instance;
}

void excutor_uring::async_io(
    CO_EVENT event, int fd, void* buf, size_t len, io_callback_t&& cb) {
    uring_impl().async_io(event, fd, buf, len, std::forward<io_callback_t>(cb));
}

void excutor_uring::stop() {
    uring_impl().stop();
}
