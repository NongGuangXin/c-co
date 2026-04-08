#include "excutor.h"
#include "log.h"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <unistd.h>

#include <liburing.h>

// 绑定线程到指定CPU核心
void bind_thread_to_cpu(std::thread& t, int cpu_id) {
    static unsigned int num_cpus = std::thread::hardware_concurrency();
    cpu_id                       = cpu_id % num_cpus;

    pthread_t pthread = t.native_handle();

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);

    int rc = pthread_setaffinity_np(pthread, sizeof(cpu_set_t), &cpuset);
    if(rc != 0) { log::erro("pthread_setaffinity_np error: {}", rc); }
}

excutor& excutor::instance() {
    static excutor inst;
    return inst;
}

excutor::excutor() {
    static size_t num_cpus = std::thread::hardware_concurrency();

    // 创建多个 io_uring 实例
    for(size_t i = 0; i < num_cpus; i++) {
        auto ui = std::make_unique<uring_instance>();

        struct io_uring_params params{};
        // 尝试使用 COOP_TASKRUN 减少内核开销（需要 kernel 5.19+）
        // params.flags = IORING_SETUP_COOP_TASKRUN;

        int ret = io_uring_queue_init_params(1024, &ui->ring, &params);
        if(ret < 0) {
            log::erro("io_uring_queue_init failed: {}", std::strerror(-ret));
            return;
        }
        log::dbug("created io_uring instance {}", i);
        urings_.push_back(std::move(ui));
    }

    // 启动 io_uring 事件线程
    for(size_t i = 0; i < num_cpus; i++) {
        uring_threads_.emplace_back([this, i]() { uring_loop(i); });
        bind_thread_to_cpu(uring_threads_.back(), i);
    }

    // 启动 worker 线程池
    for(size_t i = 0; i < DEFAULT_WORKER_THREADS; i++) {
        worker_threads_.emplace_back([this]() { worker_loop(); });
    }

    log::dbug("excutor created: {} io_uring threads, {} worker threads",
        num_cpus, DEFAULT_WORKER_THREADS);
}

excutor::~excutor() {
    running_ = false;
    queue_cv_.notify_all();

    for(auto& t: uring_threads_) {
        if(t.joinable()) {
            if(t.get_id() == std::this_thread::get_id()) {
                t.detach(); // avoid self-join deadlock (signal on uring thread)
            } else {
                t.join();
            }
        }
    }
    for(auto& t: worker_threads_) {
        if(t.joinable()) {
            if(t.get_id() == std::this_thread::get_id()) {
                t.detach();
            } else {
                t.join();
            }
        }
    }
    for(auto& ui: urings_) {
        // Free any in-flight callbacks that were never completed
        for(auto* cb: ui->inflight_cbs) { delete cb; }
        ui->inflight_cbs.clear();

        io_uring_queue_exit(&ui->ring);
    }
}

void excutor::execute(task_t task) {
    {
        std::lock_guard lock(queue_mutex_);
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
}

size_t excutor::fd_to_uring_index(int fd) const {
    return static_cast<size_t>(fd) % urings_.size();
}

// ---- 原生 io_uring 异步操作 ----

void excutor::async_recv(
    int fd, void* buf, size_t len, int flags, io_callback_t cb) {
    size_t idx = fd_to_uring_index(fd);
    auto& ui   = *urings_[idx];

    auto completion = std::make_unique<io_callback_t>(std::move(cb));

    std::lock_guard lock(ui.sq_mutex);
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ui.ring);
    if(!sqe) {
        log::erro("async_recv: io_uring_get_sqe failed fd={}", fd);
        auto moved = std::make_shared<io_callback_t>(std::move(*completion));
        execute([moved]() { (*moved)(-ENOMEM); });
        return;
    }
    io_uring_prep_recv(sqe, fd, buf, len, flags);
    auto* raw = completion.release();
    ui.inflight_cbs.insert(raw);
    io_uring_sqe_set_data(sqe, raw);
    io_uring_submit(&ui.ring);
}

void excutor::async_send(
    int fd, const void* buf, size_t len, int flags, io_callback_t cb) {
    size_t idx = fd_to_uring_index(fd);
    auto& ui   = *urings_[idx];

    auto completion = std::make_unique<io_callback_t>(std::move(cb));

    std::lock_guard lock(ui.sq_mutex);
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ui.ring);
    if(!sqe) {
        log::erro("async_send: io_uring_get_sqe failed fd={}", fd);
        auto moved = std::make_shared<io_callback_t>(std::move(*completion));
        execute([moved]() { (*moved)(-ENOMEM); });
        return;
    }
    io_uring_prep_send(sqe, fd, buf, len, flags | MSG_NOSIGNAL);
    auto* raw = completion.release();
    ui.inflight_cbs.insert(raw);
    io_uring_sqe_set_data(sqe, raw);
    io_uring_submit(&ui.ring);
}

void excutor::async_accept(
    int fd, sockaddr* addr, socklen_t* addrlen, int flags, io_callback_t cb) {
    size_t idx = fd_to_uring_index(fd);
    auto& ui   = *urings_[idx];

    auto completion = std::make_unique<io_callback_t>(std::move(cb));

    std::lock_guard lock(ui.sq_mutex);
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ui.ring);
    if(!sqe) {
        log::erro("async_accept: io_uring_get_sqe failed fd={}", fd);
        auto moved = std::make_shared<io_callback_t>(std::move(*completion));
        execute([moved]() { (*moved)(-ENOMEM); });
        return;
    }
    io_uring_prep_accept(sqe, fd, addr, addrlen, flags);
    auto* raw = completion.release();
    ui.inflight_cbs.insert(raw);
    io_uring_sqe_set_data(sqe, raw);
    io_uring_submit(&ui.ring);
}

void excutor::async_connect(
    int fd, const sockaddr* addr, socklen_t addrlen, io_callback_t cb) {
    size_t idx = fd_to_uring_index(fd);
    auto& ui   = *urings_[idx];

    auto completion = std::make_unique<io_callback_t>(std::move(cb));

    std::lock_guard lock(ui.sq_mutex);
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ui.ring);
    if(!sqe) {
        log::erro("async_connect: io_uring_get_sqe failed fd={}", fd);
        auto moved = std::make_shared<io_callback_t>(std::move(*completion));
        execute([moved]() { (*moved)(-ENOMEM); });
        return;
    }
    io_uring_prep_connect(sqe, fd, addr, addrlen);
    auto* raw = completion.release();
    ui.inflight_cbs.insert(raw);
    io_uring_sqe_set_data(sqe, raw);
    io_uring_submit(&ui.ring);
}

void excutor::async_cancel_fd(int fd) {
    size_t idx = fd_to_uring_index(fd);
    auto& ui   = *urings_[idx];

    std::lock_guard lock(ui.sq_mutex);
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ui.ring);
    if(sqe) {
        io_uring_prep_cancel_fd(sqe, fd, 0);
        io_uring_sqe_set_data(sqe, nullptr);
        io_uring_submit(&ui.ring);
    }
}

// ---- 事件循环 ----

void excutor::uring_loop(size_t index) {
    auto& ui = *urings_[index];

    while(running_) {
        struct io_uring_cqe* cqe = nullptr;

        struct __kernel_timespec ts{};
        ts.tv_sec  = 0;
        ts.tv_nsec = 100'000'000; // 100ms

        int ret = io_uring_wait_cqe_timeout(&ui.ring, &cqe, &ts);

        if(ret < 0) {
            if(ret == -ETIME || ret == -EINTR) continue;
            log::erro("io_uring_wait_cqe_timeout error on uring[{}]: {}", index,
                std::strerror(-ret));
            break;
        }

        // 批量处理所有可用的 CQE
        unsigned head;
        unsigned count = 0;
        io_uring_for_each_cqe(&ui.ring, head, cqe) {
            count++;
            auto* raw = io_uring_cqe_get_data(cqe);
            if(raw) {
                auto* cb_ptr = static_cast<io_callback_t*>(raw);
                {
                    std::lock_guard lock(ui.sq_mutex);
                    ui.inflight_cbs.erase(cb_ptr);
                }
                auto cb = std::unique_ptr<io_callback_t>(cb_ptr);
                (*cb)(cqe->res);
            }
        }
        io_uring_cq_advance(&ui.ring, count);
    }
}

void excutor::worker_loop() {
    while(true) {
        task_t task;
        {
            std::unique_lock lock(queue_mutex_);
            queue_cv_.wait(
                lock, [this]() { return !task_queue_.empty() || !running_; });
            if(!running_ && task_queue_.empty()) break;
            if(task_queue_.empty()) continue;
            task = std::move(task_queue_.front());
            task_queue_.pop();
        }
        if(task) task();
    }
}
