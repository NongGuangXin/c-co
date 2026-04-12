#include "co_excutor.h"
#include "log.h"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <mutex>
#include <thread>
#include <atomic>
#include <vector>
#include <unordered_set>

#include <liburing.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

// ---------------------------------------------------------------------------
// uring_io_context: 可池化的上下文结构体
// ---------------------------------------------------------------------------

struct uring_io_context {
    co_excutor::CO_EVENT event;
    int fd;
    void* buf;
    size_t len;
    co_excutor::io_callback_t cb;

    void reset(co_excutor::CO_EVENT ev, int f, void* b, size_t l,
        co_excutor::io_callback_t c) {
        event = ev;
        fd    = f;
        buf   = b;
        len   = l;
        cb    = std::move(c);
    }
};

// ---------------------------------------------------------------------------
// uring_instance: 独立的 ring + event_loop + per-instance 对象池 + inflight
// 跟踪
// ---------------------------------------------------------------------------

class uring_instance {
  public:
    using io_callback_t = co_excutor::io_callback_t;

    explicit uring_instance(std::atomic<bool>& running): running_(running) {
        struct io_uring_params params{};
        int ret = io_uring_queue_init_params(2048, &ring_, &params);
        if(ret < 0) {
            log::erro("excutor_uring: io_uring_queue_init failed: {}",
                std::strerror(-ret));
            throw std::runtime_error("io_uring_queue_init failed");
        }
    }

    ~uring_instance() {
        // 释放所有 in-flight ctx（不回调，进程正在退出）
        for(auto* ctx: inflight_) {
            ctx->cb  = nullptr;
            ctx->buf = nullptr;
            delete ctx;
        }
        inflight_.clear();

        io_uring_queue_exit(&ring_);
        for(auto* p: freelist_) delete p;
    }

    void async_io(co_excutor::CO_EVENT event, int fd, void* buf, size_t len,
        io_callback_t cb) {
        if(!running_.load(std::memory_order_acquire)) {
            // 进程退出中，静默丢弃，不回调（避免无限循环）
            return;
        }
        auto* ctx = acquire_ctx(event, fd, buf, len, std::move(cb));

        std::lock_guard lock(sq_mutex_);
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if(!sqe) {
            log::erro("excutor_uring: io_uring_get_sqe failed fd={}", fd);
            // auto cb_copy = std::move(ctx->cb);
            ctx->cb(-ENOMEM);
            release_ctx(ctx);
            // cb_copy(-ENOMEM);
            return;
        }

        prep_sqe(sqe, ctx, event, fd, buf, len);
        io_uring_sqe_set_data(sqe, ctx);
        io_uring_submit(&ring_);
    }

    void event_loop() {
        while(running_) {
            struct io_uring_cqe* cqe = nullptr;
            struct __kernel_timespec ts{};
            ts.tv_sec  = 0;
            ts.tv_nsec = 100'000'000; // 100ms

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

                ctx->cb(cqe->res);
                release_ctx(ctx);
            }
            io_uring_cq_advance(&ring_, count);
        }
    }

  private:
    static void prep_sqe(struct io_uring_sqe* sqe, uring_io_context* ctx,
        co_excutor::CO_EVENT event, int fd, void* buf, size_t len) {
        switch(event) {
        case co_excutor::CO_EVENT::READ: {
            io_uring_prep_recv(sqe, fd, buf, len, 0);
            break;
        }
        case co_excutor::CO_EVENT::WRITE: {
            io_uring_prep_send(sqe, fd, buf, len, MSG_NOSIGNAL);
            break;
        }
        case co_excutor::CO_EVENT::ACCEPT: {
            io_uring_prep_accept(sqe, fd,
                reinterpret_cast<struct sockaddr*>(buf),
                reinterpret_cast<socklen_t*>(&ctx->len),
                SOCK_NONBLOCK | SOCK_CLOEXEC);
            break;
        }
        case co_excutor::CO_EVENT::CONNECT: {
            io_uring_prep_connect(sqe, fd,
                reinterpret_cast<const sockaddr*>(buf),
                static_cast<socklen_t>(len));
            break;
        }
        }
    }

    // per-instance 对象池 + inflight 跟踪（用同一把锁）
    uring_io_context* acquire_ctx(co_excutor::CO_EVENT event, int fd, void* buf,
        size_t len, io_callback_t cb) {
        uring_io_context* ctx = nullptr;
        {
            std::lock_guard lock(ctx_mutex_);
            if(!freelist_.empty()) {
                ctx = freelist_.back();
                freelist_.pop_back();
            }
        }
        if(!ctx) ctx = new uring_io_context();
        ctx->reset(event, fd, buf, len, std::move(cb));
        {
            std::lock_guard lock(ctx_mutex_);
            inflight_.insert(ctx);
        }
        return ctx;
    }

    void release_ctx(uring_io_context* ctx) {
        ctx->cb  = nullptr;
        ctx->buf = nullptr;
        ctx->len = 0;
        std::lock_guard lock(ctx_mutex_);
        inflight_.erase(ctx);
        freelist_.push_back(ctx);
    }

    struct io_uring ring_{};
    std::mutex sq_mutex_;
    std::atomic<bool>& running_;

    std::mutex ctx_mutex_; // 保护 inflight_ 和 freelist_
    std::unordered_set<uring_io_context*> inflight_;
    std::vector<uring_io_context*> freelist_;
};

// ---------------------------------------------------------------------------
// excutor_uring_impl: 管理多个 uring_instance，按 fd 分片
// ---------------------------------------------------------------------------

class excutor_uring_impl {
  public:
    using io_callback_t = co_excutor::io_callback_t;

    excutor_uring_impl() {
        size_t num_cpus = std::thread::hardware_concurrency();

        for(size_t i = 0; i < num_cpus; i++) {
            instances_.push_back(std::make_unique<uring_instance>(running_));
            log::dbug("excutor_uring: created io_uring instance {}", i);
        }

        for(size_t i = 0; i < num_cpus; i++) {
            loop_threads_.emplace_back(
                [this, i]() { instances_[i]->event_loop(); });
        }

        log::dbug("excutor_uring: {} io_uring event loops started", num_cpus);
    }

    ~excutor_uring_impl() {
        running_ = false;
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
        io_callback_t cb) {
        size_t idx = static_cast<size_t>(fd) % instances_.size();
        instances_[idx]->async_io(event, fd, buf, len, std::move(cb));
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
    CO_EVENT event, int fd, void* buf, size_t len, io_callback_t cb) {
    uring_impl().async_io(event, fd, buf, len, std::move(cb));
}
