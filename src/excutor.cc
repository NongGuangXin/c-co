#include "excutor.h"

#include <array>
#include <atomic>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <condition_variable>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>

#include "fd.h"
#include "log.h"

class thrdLoop {
  public:
    thrdLoop(): thrd_(std::thread(&thrdLoop::thrd_loop, this)) { }
    ~thrdLoop() {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            stop_.store(true, std::memory_order_relaxed);
        }
        cv_.notify_one();
        if(thrd_.joinable()) { thrd_.join(); }
    }

  public:
    void execute(task_t&& task) {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if(stop_.load(std::memory_order_relaxed)) {
                log::erro("thrdLoop already stopped, cannot execute task");
                return;
            }
            task_queue_.push(std::forward<task_t>(task));
        }
        cv_.notify_one();
    }

  private:
    std::condition_variable cv_{};
    std::queue<task_t>      task_queue_{};
    std::atomic<bool>       stop_{false};
    std::thread             thrd_;
    std::mutex              mtx_{};

  private:
    void thrd_loop() {
        log::dbug("thread:{}, id:{} start", __func__, thread_id());
        while(true) {
            std::queue<task_t> local_queue;

            {
                std::unique_lock<std::mutex> lock(mtx_);
                cv_.wait(lock, [this]() {
                    return !task_queue_.empty() ||
                           stop_.load(std::memory_order_relaxed);
                });
                local_queue.swap(task_queue_);
            }

            while(local_queue.empty() == false) {
                task_t task = std::move(local_queue.front());
                local_queue.pop();
                if(task) task();
            }

            if(stop_.load(std::memory_order_relaxed)) { break; }
        }
        log::dbug("thread:{}, id:{} exit", __func__, thread_id());
    }
};

consteval auto event_array() {
    std::array<size_t, static_cast<size_t>(co_event::EVENT_MAX)> arr{};
    arr[static_cast<size_t>(co_event::READ)]  = EPOLLIN;
    arr[static_cast<size_t>(co_event::WRITE)] = EPOLLOUT;
    return arr;
}

inline constexpr auto __ep_event = event_array();

class epoller {
  public:
    epoller():
        efd_(::epoll_create1(EPOLL_CLOEXEC)),
        stop_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
        epoll_event ev;
        ev.events  = EPOLLIN | EPOLLET;
        ev.data.fd = stop_fd_;
        if(0 != ::epoll_ctl(efd_, EPOLL_CTL_ADD, stop_fd_, &ev)) {
            log::erro("epoll add error:[{}]-{}", errno, ::strerror(errno));
            throw std::runtime_error("epoller init error");
        }

        thrd_ = std::thread(&epoller::work_loop, this);
    }

    ~epoller() {
        uint64_t val = 1;
        ssize_t  n   = ::write(stop_fd_, &val, sizeof(val));
        if(n != sizeof(val)) {
            log::erro("write errno:[{}]-{}", errno, ::strerror(errno));
        }

        if(thrd_.joinable()) { thrd_.join(); }

        ::close(efd_);
        ::close(stop_fd_);

        wait_event_.clear();
    }

    void suspend(const FileDescriptor& fd, co_event co_ev, task_t&& task) {
        epoll_event ev;
        ev.events  = __ep_event[co_ev] | EPOLLET;
        ev.data.fd = fd.handle();

        if(0 != ::epoll_ctl(efd_, EPOLL_CTL_ADD, ev.data.fd, &ev)) {
            if(errno == EEXIST) {
                if(0 != ::epoll_ctl(efd_, EPOLL_CTL_MOD, ev.data.fd, &ev)) {
                    log::erro(
                        "epoll mod error:[{}]-{}", errno, ::strerror(errno));
                }
            } else {
                log::erro("epoll add error:[{}]-{}", errno, ::strerror(errno));
            }
        }
        wait_event_[ev.data.fd] = std::move(task);
    }

  private:
    int                             efd_{-1};
    int                             stop_fd_{-1};
    std::thread                     thrd_;
    std::unordered_map<int, task_t> wait_event_;

    void work_loop() {
        log::dbug("epoll loop start");
        constexpr int maxevents = 1024;
        epoll_event   events[maxevents];

        while(true) {
            int n = ::epoll_wait(efd_, events, maxevents, -1);
            if(n < 0) {
                if(errno == EBADF) {
                    break; // by close(epfd)
                }
                if(errno == EINTR) { continue; }
                log::erro("epoll wait error:[{}]-{}", errno, ::strerror(errno));
            }

            if(n == 0) { /* timeout */
                continue;
            }

            for(int i = 0; i < n; i++) {
                int fd = events[i].data.fd;

                if(fd == stop_fd_) {
                    for(auto& [fd, task]: wait_event_) {
                        excutor::instance().execute(task);
                    }
                    return;
                }

                auto it = wait_event_.find(fd);
                if(it == wait_event_.end()) { continue; }

                if(::epoll_ctl(efd_, EPOLL_CTL_DEL, fd, nullptr) != 0) {
                    log::erro(
                        "epoll del error:[{}]-{}", errno, ::strerror(errno));
                }
                task_t task = std::move(it->second);
                wait_event_.erase(it);
                excutor::instance().execute(task);
            }
        }
        return;
    }
};

class excutor::excutorImpl {
  public:
    void execute(task_t&& task) {
        thrd_.execute(std::forward<task_t>(task));
    }

    void suspend(const FileDescriptor& fd, co_event ev, task_t&& task) {
        epoller_.suspend(fd, ev, std::forward<task_t>(task));
    }

  private:
    thrdLoop thrd_;
    epoller  epoller_;
};

void excutor::execute(task_t task) {
    impl_->execute(std::forward<task_t>(task));
}

void excutor::suspend(const FileDescriptor& fd, co_event ev, task_t task) {
    impl_->suspend(fd, ev, std::forward<task_t>(task));
}

excutor::excutor(): impl_(std::make_unique<excutorImpl>()) { }
excutor::~excutor() = default;

std::string thread_id() {
    std::thread::id    thrd_id = std::this_thread::get_id();
    std::ostringstream oss;
    oss << thrd_id;
    return oss.str();
}
