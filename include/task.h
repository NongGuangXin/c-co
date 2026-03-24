#pragma once

#include <condition_variable>
#include <coroutine>
#include <exception>
#include <optional>
#include <utility>
#include <mutex>

#include "log.h"
#include "excutor.h"

template <typename T>
class task {
  public:
    struct promise_type {
      public:
        std::optional<T>        value_{};
        std::exception_ptr      exception_{};
        std::coroutine_handle<> continuation_{};

        bool done_{false};
        // 用于 sync_wait 的通知
        std::mutex*              wait_mutex_{nullptr};
        std::condition_variable* wait_cv_{nullptr};

      public:
        task get_return_object() {
            log::dbug("task-promise:{}", __func__);
            return task{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() noexcept {
            log::dbug("task-promise:{}", __func__);
            return {};
        }

        struct final_awaiter {
            bool await_ready() const noexcept {
                log::dbug("task-final_awaiter-await_ready:{}", __func__);
                return false;
            }

            std::coroutine_handle<> await_suspend(
                std::coroutine_handle<promise_type> h) noexcept {
                log::dbug("task-final_awaiter-await_suspend:{}", __func__);
                auto& promise = h.promise();
                promise.done_ = true;

                // 通知 sync_wait
                if(promise.wait_mutex_ && promise.wait_cv_) {
                    std::lock_guard<std::mutex> lock(*promise.wait_mutex_);
                    promise.wait_cv_->notify_all();
                }

                if(promise.continuation_) { return promise.continuation_; }

                return std::noop_coroutine();
            }

            void await_resume() noexcept {
                log::dbug("task-final_awaiter-await_resume:{}", __func__);
            }
        };

        final_awaiter final_suspend() noexcept {
            log::dbug("task-promise:{}", __func__);
            return {};
        }

        void return_value(T value) {
            log::dbug("task-promise:{}", __func__);
            value_ = std::move(value);
        }

        void unhandled_exception() {
            log::dbug("task-promise:{}", __func__);
            exception_ = std::current_exception();
        }
    };

  public:
    using handle_type = std::coroutine_handle<promise_type>;

    explicit task(handle_type h): handle_(h) { }
    task(const task&)            = delete;
    task& operator=(const task&) = delete;

    task(task&& other) noexcept: handle_(std::exchange(other.handle_, {})) { }
    task& operator=(task&& other) noexcept {
        if(this != &other) {
            if(handle_) { handle_.destroy(); }
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    ~task() {
        if(handle_) { handle_.destroy(); }
    }

    bool is_done() const {
        return handle_ && handle_.promise().done_;
    }

    T value() {
        if(handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
        return std::move(*handle_.promise().value_);
    }

    handle_type handle() const {
        return handle_;
    }

    auto operator co_await() {
        struct awaiter {
            handle_type handle_;

            bool await_ready() const noexcept {
                log::dbug("task-co_await-await_ready:{}", __func__);
                return handle_.promise().done_;
            }

            void await_suspend(std::coroutine_handle<> continuation) {
                log::dbug("task-co_await-await_suspend:{}", __func__);
                handle_.promise().continuation_ = continuation;

                excutor::instance().execute([h = handle_]() {
                    if(h && !h.done()) { h.resume(); }
                });
            }

            T await_resume() {
                log::dbug("task-co_await-await_resume:{}", __func__);
                if(handle_.promise().exception_) {
                    std::rethrow_exception(handle_.promise().exception_);
                }
                return std::move(*handle_.promise().value_);
            }
        };

        return awaiter{handle_};
    }

  private:
    handle_type handle_;
};

template <typename T>
T sync_wait(task<T>&& t) {
    auto& promise = t.handle().promise();

    std::mutex              wait_mutex;
    std::condition_variable wait_cv;
    promise.wait_mutex_ = &wait_mutex;
    promise.wait_cv_    = &wait_cv;

    auto handle = t.handle();

    excutor::instance().execute([handle]() {
        if(handle && !handle.done()) { handle.resume(); }
    });

    // 使用 condition_variable 等待协程完成
    std::unique_lock<std::mutex> lock(wait_mutex);
    wait_cv.wait(lock, [&promise] { return promise.done_; });
    return t.value();
}
