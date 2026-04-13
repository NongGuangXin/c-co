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

#include <liburing.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>

// -------------------------------------
// uring_io_context: 可池化的上下文结构体
// -------------------------------------
struct uring_io_context {
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

// ---------------------------------------------------------------------------
// uring_instance: 独立的 ring + event_loop + per-instance 对象池
// ---------------------------------------------------------------------------
class uring_instance {
  public:
    explicit uring_instance(std::atomic<bool>& running): running_(running) {
        struct io_uring_params params{};
        int rc = io_uring_queue_init_params(4096, &ring_, &params);
        if(rc < 0) {
            log::erro("excutor_uring: io_uring_queue_init failed: {}",
                std::strerror(-rc));
            throw std::runtime_error("io_uring_queue_init failed");
        }
    }

    ~uring_instance() {
        io_uring_queue_exit(&ring_);
        for(auto* p: all_allocated_) { delete p; }
    }

    void async_io(
        CO_EVENT event, int fd, void* buf, size_t len, io_callback_t&& cb) {
        if(!running_.load(std::memory_order_relaxed)) { return; }

        auto* ctx  = acquire_ctx();
        ctx->event = event;
        ctx->fd    = fd;
        ctx->buf   = buf;
        ctx->len   = len;
        ctx->cb    = std::move(cb);

        std::lock_guard lock(sq_mutex_);
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if(!sqe) {
            // SQ full - force submit and retry
            io_uring_submit(&ring_);
            sqe = io_uring_get_sqe(&ring_);
            if(!sqe) {
                log::erro("excutor_uring: io_uring_get_sqe failed fd={}", fd);
                ctx->cb(-ENOMEM);
                release_ctx(ctx);
                return;
            }
        }

        prep_sqe(sqe, ctx, event, fd, buf, len);
        io_uring_sqe_set_data(sqe, ctx);
        io_uring_submit(&ring_);
    }

    void event_loop() {
        while(running_.load(std::memory_order_relaxed)) {
            struct io_uring_cqe* cqe = nullptr;
            struct __kernel_timespec ts{};
            ts.tv_sec  = 0;
            ts.tv_nsec = 5'000'000; // 5ms

            int ret = io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
            if(ret < 0) {
                if(ret == -ETIME || ret == -EINTR) continue;
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
                if(!ctx) continue;

                // If shutting down, don't invoke callbacks - just free context
                if(!running_.load(std::memory_order_relaxed)) {
                    release_ctx(ctx);
                    continue;
                }

                if(ctx->event == CO_EVENT::ACCEPT && cqe->res >= 0) {
                    optimize_socket(cqe->res);
                }

                ctx->cb(cqe->res);
                release_ctx(ctx);
            }
            io_uring_cq_advance(&ring_, count);
        }
    }

  private:
    static void prep_sqe(struct io_uring_sqe* sqe, uring_io_context* ctx,
        CO_EVENT event, int fd, void* buf, size_t len) {
        switch(event) {
        case CO_EVENT::RECV: {
            io_uring_prep_recv(sqe, fd, buf, len, 0);
            break;
        }
        case CO_EVENT::SEND: {
            io_uring_prep_send(sqe, fd, buf, len, MSG_NOSIGNAL);
            break;
        }
        case CO_EVENT::ACCEPT: {
            io_uring_prep_accept(sqe, fd,
                reinterpret_cast<struct sockaddr*>(buf),
                reinterpret_cast<socklen_t*>(&ctx->len),
                SOCK_NONBLOCK | SOCK_CLOEXEC);
            break;
        }
        case CO_EVENT::CONNECT: {
            io_uring_prep_connect(sqe, fd,
                reinterpret_cast<const sockaddr*>(buf),
                static_cast<socklen_t>(len));
            break;
        }
        }
    }

    uring_io_context* acquire_ctx() {
        uring_io_context* ctx = nullptr;
        {
            std::lock_guard lock(ctx_mutex_);
            if(!freelist_.empty()) {
                ctx = freelist_.back();
                freelist_.pop_back();
            }
        }
        if(!ctx) {
            ctx = new uring_io_context();
            std::lock_guard lock(ctx_mutex_);
            all_allocated_.push_back(ctx);
        }
        return ctx;
    }

    void release_ctx(uring_io_context* ctx) {
        ctx->cb  = nullptr;
        ctx->buf = nullptr;
        ctx->len = 0;
        std::lock_guard lock(ctx_mutex_);
        freelist_.push_back(ctx);
    }

    struct io_uring ring_{};
    std::mutex sq_mutex_;
    std::atomic<bool>& running_;

    std::mutex ctx_mutex_;
    std::vector<uring_io_context*> freelist_;
    std::vector<uring_io_context*> all_allocated_;
};

// ---------------------------------------------------------------------------
// excutor_uring_impl: 管理多个 uring_instance，按 fd 分片
// ---------------------------------------------------------------------------
class excutor_uring_impl {
    constexpr static size_t num_cpus = 4;

  public:
    excutor_uring_impl() {
        // size_t num_cpus = std::thread::hardware_concurrency();
        for(size_t i = 0; i < num_cpus; i++) {
            instances_.push_back(std::make_unique<uring_instance>(running_));
            log::dbug("excutor_uring: created io_uring instance {}", i);
        }

        for(size_t i = 0; i < num_cpus; i++) {
            loop_threads_.emplace_back(
                [this, i]() { instances_[i]->event_loop(); });
            // bind_thread_to_cpu(loop_threads_.back(), static_cast<int>(i));
        }

        log::dbug("excutor_uring: {} io_uring event loops started", num_cpus);
    }

    ~excutor_uring_impl() {
        stop();
    }

    void stop() {
        bool expected = true;
        if(!running_.compare_exchange_strong(expected, false)) { return; }
        for(auto& t: loop_threads_) {
            if(t.joinable()) {
                t.join();
                // if(t.get_id() == std::this_thread::get_id()) {
                //     t.detach();
                // } else {
                //     t.join();
                // }
            }
        }
    }

    void async_io(
        CO_EVENT event, int fd, void* buf, size_t len, io_callback_t cb) {
        size_t idx = static_cast<size_t>(fd) % num_cpus;
        instances_[idx]->async_io(
            event, fd, buf, len, std::forward<io_callback_t>(cb));
    }

  private:
    std::vector<std::unique_ptr<uring_instance>> instances_;
    std::vector<std::thread> loop_threads_;
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
