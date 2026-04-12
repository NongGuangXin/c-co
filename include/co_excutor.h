#pragma once

#include "task.h"
#include "thrdpool.h"

#include <future>
#include <functional>

using task_t        = std::function<void()>;
using io_callback_t = std::function<void(int)>;

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

    virtual void async_io(
        CO_EVENT event, int fd, void* buf, size_t len, io_callback_t&& cb) = 0;

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

    // 分离协程
    template <typename T>
    static void detach(task<T>&& t) {
        auto dt = std::make_shared<task<T>>(std::move(t));

        auto wrapper = [](std::shared_ptr<task<T>> inner) -> task<void> {
            try {
                co_await std::move(*inner);
            } catch(...) {
                // swallow
            }
        }(std::move(dt));

        // Mark wrapper for self-destruction at final_suspend
        auto h                    = wrapper.handle();
        h.promise().self_destroy_ = true;
        wrapper.release(); // release ownership, frame self-destroys

        instance().execute([h]() mutable { h.resume(); });
    }

  private:
    thrdpool pool;
};

class excutor_epoll : public co_excutor {
  public:
    void async_io(CO_EVENT event, int fd, void* buf, size_t len,
        io_callback_t&& cb) override;
};

class excutor_uring : public co_excutor {
    void async_io(CO_EVENT event, int fd, void* buf, size_t len,
        io_callback_t&& cb) override;
};

void bind_thread_to_cpu(std::thread& t, int cpu_id);
