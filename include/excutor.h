#pragma once

#include "task.h"

#include <atomic>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include <functional>
#include <unordered_map>
#include <condition_variable>
#include <cstddef>

#include <sys/epoll.h>
#include <unistd.h>

class FileDescriptor {
  public:
    FileDescriptor() = default;

    FileDescriptor(int fd):
        fd_(new int(fd), [](int* fd) {
            if(fd != nullptr && *fd >= 0) { ::close(*fd); }
            if(fd != nullptr) { delete fd; }
        }) { }

    FileDescriptor(const FileDescriptor&) noexcept            = default;
    FileDescriptor& operator=(const FileDescriptor&) noexcept = default;

    FileDescriptor(FileDescriptor&&) noexcept            = default;
    FileDescriptor& operator=(FileDescriptor&&) noexcept = default;

  public:
    int handle() const noexcept {
        return fd_ ? *fd_ : -1;
    }

    explicit operator bool() const noexcept {
        return fd_ != nullptr && *fd_ >= 0;
    }

    bool operator==(const FileDescriptor& other) const noexcept {
        return handle() == other.handle();
    }

  private:
    std::shared_ptr<int> fd_{nullptr};
};

class excutor {
  public:
    using task_t = std::function<void()>;

    enum co_event {
        READ,
        WRITE,
    };

    static constexpr size_t DEFAULT_WORKER_THREADS = 2;

    static excutor& instance();

    ~excutor();

    excutor(const excutor&)            = delete;
    excutor& operator=(const excutor&) = delete;

    // 调度执行任务到线程池
    void execute(task_t task);

    // 注册事件（fd 会被分配到某个 epoll 实例）
    void register_event(const FileDescriptor& fd, co_event ev, task_t task);

    // 注销事件
    void unregister_event(const FileDescriptor& fd);

    // 带返回值调度任务
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
    excutor();

    // 每个 epoll 实例的数据
    struct epoll_instance {
        int epoll_fd{-1};
        std::mutex mutex;
        std::unordered_map<int, task_t> callbacks;
    };

    void epoll_loop(size_t index);
    void worker_loop();

    // 根据 fd 选择 epoll 实例（一致性分配）
    size_t fd_to_epoll_index(int fd) const;

  private:
    std::atomic<bool> running_{true};

    // 多 epoll 实例
    std::vector<std::unique_ptr<epoll_instance>> epollers_;
    std::vector<std::thread> epoll_threads_;

    // 线程池
    std::vector<std::thread> worker_threads_;
    std::condition_variable queue_cv_;
    std::queue<task_t> task_queue_;
    std::mutex queue_mutex_;
};
