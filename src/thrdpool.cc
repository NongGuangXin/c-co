#include "thrdpool.h"
#include <atomic>

thrdpool::thrdpool(size_t num_threads) {
    for(size_t i = 0; i < num_threads; i++) {
        worker_threads_.emplace_back([this]() { worker_loop(); });
    }
}

thrdpool::~thrdpool() {
    running_.store(false);
    queue_cv_.notify_all();

    for(std::thread& thrd : worker_threads_) {
        thrd.join();
    }
}

void thrdpool::execute(task_t&& task) {
    {
        std::lock_guard lock(queue_mutex_);
        task_queue_.push(std::forward<task_t>(task));
    }
    queue_cv_.notify_one();
}

void thrdpool::worker_loop() {
    while(true) {
        task_t task;
        {
            std::unique_lock lock(queue_mutex_);
            queue_cv_.wait(lock, [this]() { return !task_queue_.empty() || !running_.load(); });
            if(!running_.load()) {
                break;
            }
            task = std::move(task_queue_.front());
            task_queue_.pop();
        }
        if(task) task();
    }

    while (!task_queue_.empty()) {
        task_t task = std::move(task_queue_.front());
        task_queue_.pop();
        if(task) task();
    }
}
