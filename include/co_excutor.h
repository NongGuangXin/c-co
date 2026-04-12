#pragma once

#include "task.h"
#include "thrdpool.h"

#include <future>
#include <functional>
#include <cstring>
#include <type_traits>
#include <mutex>
#include <vector>
#include <coroutine>
#include <algorithm>

using task_t = std::function<void()>;

// Registry for detached coroutine handles that need cleanup on shutdown.
// Detached coroutines self-destroy at final_suspend; this registry tracks
// those still suspended so we can destroy them during process exit.
class detached_registry {
  public:
    static detached_registry& instance() {
        static detached_registry reg;
        return reg;
    }

    void add(std::coroutine_handle<> h) {
        std::lock_guard lock(mutex_);
        handles_.push_back(h);
    }

    void remove(std::coroutine_handle<> h) {
        std::lock_guard lock(mutex_);
        auto it = std::find(handles_.begin(), handles_.end(), h);
        if(it != handles_.end()) {
            *it = handles_.back();
            handles_.pop_back();
        }
    }

    void destroy_all() {
        std::lock_guard lock(mutex_);
        for(auto h: handles_) {
            if(h && !h.done()) h.destroy();
        }
        handles_.clear();
    }

    ~detached_registry() {
        destroy_all();
    }

  private:
    detached_registry() = default;
    std::mutex mutex_;
    std::vector<std::coroutine_handle<>> handles_;
};

// Lightweight type-erased callback for IO completions.
// Uses inline buffer (48 bytes) to avoid heap allocation for typical lambda
// captures. Falls back to std::function for larger captures.
// 觉得太复杂可以直接：using io_callback_t = std::function<void(int)>;
class io_callback_t {
    static constexpr size_t BUF_SIZE = 48;

    using invoke_fn  = void (*)(void*, int);
    using destroy_fn = void (*)(void*);
    using move_fn    = void (*)(void* src, void* dst);

    alignas(std::max_align_t) unsigned char buf_[BUF_SIZE]{};
    invoke_fn invoke_{nullptr};
    destroy_fn destroy_{nullptr};
    move_fn move_{nullptr};

    void clear() noexcept {
        if(destroy_) destroy_(buf_);
        invoke_  = nullptr;
        destroy_ = nullptr;
        move_    = nullptr;
    }

  public:
    io_callback_t() = default;

    template <typename F, typename = std::enable_if_t<
                              !std::is_same_v<std::decay_t<F>, io_callback_t>>>
    io_callback_t(F&& f) {
        using Fn = std::decay_t<F>;
        static_assert(sizeof(Fn) <= BUF_SIZE,
            "io_callback_t: callable too large for inline buffer");
        static_assert(std::is_nothrow_move_constructible_v<Fn>,
            "io_callback_t: callable must be nothrow move constructible");
        ::new(buf_) Fn(std::forward<F>(f));
        invoke_ = [](void* p, int res) {
            (*static_cast<Fn*>(p))(res);
        };
        destroy_ = [](void* p) {
            static_cast<Fn*>(p)->~Fn();
        };
        move_ = [](void* src, void* dst) {
            ::new(dst) Fn(std::move(*static_cast<Fn*>(src)));
            static_cast<Fn*>(src)->~Fn();
        };
    }

    io_callback_t(io_callback_t&& other) noexcept {
        if(other.move_) {
            other.move_(other.buf_, buf_);
            invoke_        = other.invoke_;
            destroy_       = other.destroy_;
            move_          = other.move_;
            other.invoke_  = nullptr;
            other.destroy_ = nullptr;
            other.move_    = nullptr;
        }
    }

    io_callback_t& operator=(io_callback_t&& other) noexcept {
        if(this != &other) {
            clear();
            if(other.move_) {
                other.move_(other.buf_, buf_);
                invoke_        = other.invoke_;
                destroy_       = other.destroy_;
                move_          = other.move_;
                other.invoke_  = nullptr;
                other.destroy_ = nullptr;
                other.move_    = nullptr;
            }
        }
        return *this;
    }

    io_callback_t(std::nullptr_t) noexcept { }

    io_callback_t& operator=(std::nullptr_t) noexcept {
        clear();
        return *this;
    }

    ~io_callback_t() {
        clear();
    }

    explicit operator bool() const noexcept {
        return invoke_ != nullptr;
    }

    void operator()(int res) {
        invoke_(buf_, res);
    }
};

class co_excutor {
  public:
    enum class CO_EVENT : unsigned char {
        READ    = 0x1,
        WRITE   = 0x2,
        ACCEPT  = 0x4,
        CONNECT = 0x8
    };

    co_excutor()          = default;
    virtual ~co_excutor() = default;

    static co_excutor& instance();

    // Orderly shutdown: stop event loops, then destroy detached coroutines.
    // Called from signal handler before exit.
    static void shutdown();

    virtual void async_io(
        CO_EVENT event, int fd, void* buf, size_t len, io_callback_t&& cb) = 0;

    // Stop backend event loops (idempotent). Called during orderly shutdown.
    virtual void stop() { }

    virtual void execute(task_t&& task);

    template <typename F, typename... Args,
        typename R = std::invoke_result_t<F, Args...>>
    std::future<R> submit(F&& f, Args&&... args) {
        auto packaged = std::make_shared<std::packaged_task<R()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));
        execute([packaged]() { (*packaged)(); });
        return packaged->get_future();
    }

    // 同步等待协程
    template <typename T>
    static T sync_wait(task<T>&& t) {
        std::promise<T> promise;
        auto future = promise.get_future();

        auto wrapper = [](task<T> inner, std::promise<T> p) -> task<void> {
            try {
                if constexpr(std::is_void_v<T>) {
                    co_await std::move(inner);
                    p.set_value();
                } else {
                    auto result = co_await std::move(inner);
                    p.set_value(std::move(result));
                }
            } catch(...) { p.set_exception(std::current_exception()); }
        }(std::move(t), std::move(promise));

        // Mark wrapper for self-destruction at final_suspend
        auto h                    = wrapper.handle();
        h.promise().self_destroy_ = true;
        wrapper.release(); // release ownership, frame self-destroys

        instance().execute([h]() mutable { h.resume(); });

        return future.get();
    }

    // 分离协程 - 直接移动 task 到协程帧中，避免 shared_ptr 开销
    template <typename T>
    static void detach(task<T>&& t) {
        auto wrapper = [](task<T> inner) -> task<void> {
            try {
                co_await std::move(inner);
            } catch(...) {
                // swallow
            }
        }(std::move(t));

        // Mark wrapper for self-destruction at final_suspend
        auto h                    = wrapper.handle();
        h.promise().self_destroy_ = true;
        h.promise().on_destroy_   = [](std::coroutine_handle<> handle) {
            detached_registry::instance().remove(handle);
        };
        wrapper.release(); // release ownership, frame self-destroys

        detached_registry::instance().add(h);
        instance().execute([h]() mutable { h.resume(); });
    }

  private:
    thrdpool pool;
};

class excutor_epoll : public co_excutor {
  public:
    void async_io(CO_EVENT event, int fd, void* buf, size_t len,
        io_callback_t&& cb) override;
    void stop() override;
};

class excutor_uring : public co_excutor {
  public:
    void async_io(CO_EVENT event, int fd, void* buf, size_t len,
        io_callback_t&& cb) override;
    void stop() override;
};

void bind_thread_to_cpu(std::thread& t, int cpu_id);
