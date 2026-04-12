#pragma once

#include <condition_variable>
#include <functional>
#include <queue>
#include <thread>
class thrdpool final {
  public:
    explicit thrdpool(size_t num_threads = 4);
    ~thrdpool();

    using task_t = std::function<void()>;

    void execute(task_t&& task);

  private:
    std::vector<std::thread> worker_threads_{};
    std::condition_variable queue_cv_{};
    std::atomic<bool> running_{true};
    std::queue<task_t> task_queue_{};
    std::mutex queue_mutex_{};

    void worker_loop();
};
