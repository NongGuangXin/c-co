#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <utility>

template <typename T = void>
class task;

namespace detail {

struct task_promise_base {
    std::coroutine_handle<> continuation_{nullptr};
    std::exception_ptr exception_{nullptr};
    bool self_destroy_{false};

    auto initial_suspend() noexcept {
        return std::suspend_always{};
    }

    struct final_awaiter {
        bool await_ready() noexcept {
            return false;
        }

        template <typename Promise>
        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<Promise> h) noexcept {
            auto& promise = h.promise();
            auto cont     = promise.continuation_;

            if(promise.self_destroy_) {
                // Coroutine owns itself - destroy frame here
                // Must capture continuation before destroy
                h.destroy();
            }

            if(cont) return cont;
            return std::noop_coroutine();
        }

        void await_resume() noexcept { }
    };

    auto final_suspend() noexcept {
        return final_awaiter{};
    }

    void unhandled_exception() {
        exception_ = std::current_exception();
    }
};

template <typename T>
struct task_promise : task_promise_base {
    std::optional<T> result_;

    task<T> get_return_object();

    void return_value(T value) {
        result_ = std::move(value);
    }

    T& result() {
        if(exception_) { std::rethrow_exception(exception_); }
        return *result_;
    }
};

template <>
struct task_promise<void> : task_promise_base {
    task<void> get_return_object();

    void return_void() { }

    void result() {
        if(exception_) { std::rethrow_exception(exception_); }
    }
};

} // namespace detail

template <typename T>
class task {
  public:
    using promise_type = detail::task_promise<T>;
    using handle_type  = std::coroutine_handle<promise_type>;

    explicit task(handle_type h): handle_(h) { }

    task(const task&)            = delete;
    task& operator=(const task&) = delete;

    task(task&& other) noexcept:
        handle_(std::exchange(other.handle_, nullptr)) { }

    task& operator=(task&& other) noexcept {
        if(this != &other) {
            if(handle_) { handle_.destroy(); }
            handle_ = std::exchange(other.handle_, nullptr);
        }
        return *this;
    }

    ~task() {
        if(handle_) { handle_.destroy(); }
    }

    handle_type handle() const {
        return handle_;
    }

    // Release ownership of the coroutine handle (caller takes responsibility)
    handle_type release() noexcept {
        return std::exchange(handle_, nullptr);
    }

    // Awaitable interface
    bool await_ready() const noexcept {
        return false;
    }

    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<> awaiting) noexcept {
        handle_.promise().continuation_ = awaiting;
        return handle_;
    }

    decltype(auto) await_resume() {
        return handle_.promise().result();
    }

  private:
    handle_type handle_{nullptr};
};

namespace detail {

template <typename T>
task<T> task_promise<T>::get_return_object() {
    return task<T>{std::coroutine_handle<task_promise<T>>::from_promise(*this)};
}

inline task<void> task_promise<void>::get_return_object() {
    return task<void>{
        std::coroutine_handle<task_promise<void>>::from_promise(*this)};
}

} // namespace detail
