#include "excutor.h"
#include "log.h"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <unistd.h>
#include <sys/epoll.h>

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

    // 创建多个 epoll 实例
    for(size_t i = 0; i < num_cpus; i++) {
        auto ep      = std::make_unique<epoll_instance>();
        ep->epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
        if(ep->epoll_fd < 0) {
            log::erro("epoll_create1 failed: {}", std::strerror(errno));
            return;
        }
        log::dbug("created epoll instance {}, fd={}", i, ep->epoll_fd);
        epollers_.push_back(std::move(ep));
    }

    // 启动 epoll 事件线程
    for(size_t i = 0; i < num_cpus; i++) {
        epoll_threads_.emplace_back([this, i]() { epoll_loop(i); });
        bind_thread_to_cpu(epoll_threads_.back(), i);
    }

    // 启动 worker 线程池
    for(size_t i = 0; i < DEFAULT_WORKER_THREADS; i++) {
        worker_threads_.emplace_back([this]() { worker_loop(); });
    }

    log::dbug("excutor created: {} epoll threads, {} worker threads", num_cpus,
        DEFAULT_WORKER_THREADS);
}

excutor::~excutor() {
    running_ = false;
    queue_cv_.notify_all();

    for(auto& t: epoll_threads_) {
        if(t.joinable()) t.join();
    }
    for(auto& t: worker_threads_) {
        if(t.joinable()) t.join();
    }
    for(auto& ep: epollers_) {
        if(ep->epoll_fd >= 0) ::close(ep->epoll_fd);
    }
}

void excutor::execute(task_t task) {
    {
        std::lock_guard lock(queue_mutex_);
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
}

size_t excutor::fd_to_epoll_index(int fd) const {
    return static_cast<size_t>(fd) % epollers_.size();
}

void excutor::register_event(
    const FileDescriptor& fd, co_event ev, task_t task) {
    int raw_fd = fd.handle();
    size_t idx = fd_to_epoll_index(raw_fd);
    auto& ep   = *epollers_[idx];

    log::dbug("register_event fd={} ev={} epoll[{}]", raw_fd,
        ev == READ ? "READ" : "WRITE", idx);

    struct epoll_event epev{};
    epev.events = (ev == READ) ? EPOLLIN : EPOLLOUT;
    epev.events |= EPOLLONESHOT | EPOLLET;
    epev.data.fd = raw_fd;

    // 先插入 callback 再注册 epoll，确保事件触发时 callback 已就绪
    {
        std::lock_guard lock(ep.mutex);
        ep.callbacks[raw_fd] = std::move(task);
    }

    // 先 ADD（常规路径：首次注册或 EPOLLONESHOT 消费后重新注册）
    // EEXIST 时 MOD（短写循环：fd 仍在 epoll 中但 oneshot 已禁用，需重新激活）
    bool ok = true;
    if(::epoll_ctl(ep.epoll_fd, EPOLL_CTL_ADD, raw_fd, &epev) < 0) {
        if(errno == EEXIST) {
            if(::epoll_ctl(ep.epoll_fd, EPOLL_CTL_MOD, raw_fd, &epev) < 0) {
                log::erro("epoll_ctl MOD failed fd={}: {}", raw_fd,
                    std::strerror(errno));
                ok = false;
            }
        } else {
            log::erro(
                "epoll_ctl ADD failed fd={}: {}", raw_fd, std::strerror(errno));
            ok = false;
        }
    }

    // epoll_ctl 失败时，取回 callback 并在线程池中执行，避免协程永久挂起
    if(!ok) {
        task_t cb;
        {
            std::lock_guard lock(ep.mutex);
            auto it = ep.callbacks.find(raw_fd);
            if(it != ep.callbacks.end()) {
                cb = std::move(it->second);
                ep.callbacks.erase(it);
            }
        }
        if(cb) {
            log::warn("executing callback for failed epoll_ctl fd={}", raw_fd);
            execute(std::move(cb));
        }
    }
}

void excutor::unregister_event(const FileDescriptor& fd) {
    int raw_fd = fd.handle();
    size_t idx = fd_to_epoll_index(raw_fd);
    auto& ep   = *epollers_[idx];

    log::dbug("unregister_event fd={} epoll[{}]", raw_fd, idx);

    ::epoll_ctl(ep.epoll_fd, EPOLL_CTL_DEL, raw_fd, nullptr);

    std::lock_guard lock(ep.mutex);
    ep.callbacks.erase(raw_fd);
}

void excutor::epoll_loop(size_t index) {
    constexpr int MAX_EVENTS = 64;
    struct epoll_event events[MAX_EVENTS];
    auto& ep = *epollers_[index];

    while(running_) {
        int n = ::epoll_wait(ep.epoll_fd, events, MAX_EVENTS, 100);
        if(n < 0) {
            if(errno == EINTR) continue;
            log::erro("epoll_wait error on epoll[{}]: {}", index,
                std::strerror(errno));
            break;
        }

        for(int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            task_t cb;
            {
                std::lock_guard lock(ep.mutex);
                auto it = ep.callbacks.find(fd);
                if(it != ep.callbacks.end()) {
                    cb = std::move(it->second);
                    ep.callbacks.erase(it);
                }
            }
            if(cb) {
                // 将IO回调投递到线程池执行，可以避免阻塞epoll线程,
                // 但是IO性能会降低 execute(std::move(cb));
                cb();
            }
        }
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
