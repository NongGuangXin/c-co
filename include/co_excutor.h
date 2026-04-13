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

using task_t        = std::function<void()>;
using io_callback_t = std::function<void(int)>;

enum class CO_EVENT : unsigned char {
    RECV    = 0x1,
    SEND    = 0x2,
    ACCEPT  = 0x4,
    CONNECT = 0x8
};

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

class co_excutor {
  public:
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
