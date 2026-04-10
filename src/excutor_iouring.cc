#include "co_excutor.h"
#include "log.h"

#include <cerrno>
#include <cstring>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <atomic>
#include <vector>

#include <liburing.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

class excutor_uring_impl {
  public:
    using io_callback_t = co_excutor::io_callback_t;

    excutor_uring_impl() {
        size_t num_cpus = std::thread::hardware_concurrency();

        for(size_t i = 0; i < num_cpus; i++) {
            auto ui = std::make_unique<uring_instance>();
            struct io_uring_params params{};

            int ret = io_uring_queue_init_params(1024, &ui->ring, &params);
            if(ret < 0) {
                log::erro("excutor_uring: io_uring_queue_init failed: {}",
                    std::strerror(-ret));
                return;
            }
            log::dbug("excutor_uring: created io_uring instance {}", i);
            urings_.push_back(std::move(ui));
        }

        for(size_t i = 0; i < num_cpus; i++) {
            loop_threads_.emplace_back([this, i]() { event_loop(i); });
        }

        log::dbug("excutor_uring: {} io_uring event loops started", num_cpus);
    }

    ~excutor_uring_impl() {
        running_ = false;
        for(auto& t: loop_threads_) {
            if(t.joinable()) {
                if(t.get_id() == std::this_thread::get_id()) {
                    t.detach();
                } else {
                    t.join();
                }
            }
        }
        for(auto& ui: urings_) {
            for(auto* cb: ui->inflight_cbs) { delete cb; }
            ui->inflight_cbs.clear();
            io_uring_queue_exit(&ui->ring);
        }
    }

    void async_io(co_excutor::CO_EVENT event, int fd,
        std::vector<unsigned char>& buf, io_callback_t cb, ssize_t len = -1) {
        size_t idx = static_cast<size_t>(fd) % urings_.size();
        auto& ui   = *urings_[idx];

        auto completion = std::make_unique<io_callback_t>(std::move(cb));

        std::lock_guard lock(ui.sq_mutex);
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ui.ring);
        if(!sqe) {
            log::erro("excutor_uring: io_uring_get_sqe failed fd={}", fd);
            auto moved =
                std::make_shared<io_callback_t>(std::move(*completion));
            (*moved)(-ENOMEM);
            return;
        }

        switch(event) {
        case co_excutor::CO_EVENT::READ: {
            size_t nbytes = (len > 0) ? static_cast<size_t>(len) : buf.size();
            nbytes        = std::min(nbytes, buf.size());
            io_uring_prep_recv(sqe, fd, buf.data(), nbytes, 0);
            break;
        }
        case co_excutor::CO_EVENT::WRITE: {
            size_t nbytes = (len > 0) ? static_cast<size_t>(len) : buf.size();
            nbytes        = std::min(nbytes, buf.size());
            io_uring_prep_send(sqe, fd, buf.data(), nbytes, MSG_NOSIGNAL);
            break;
        }
        case co_excutor::CO_EVENT::ACCEPT: {
            socklen_t len = static_cast<socklen_t>(buf.size());
            io_uring_prep_accept(sqe, fd,
                reinterpret_cast<struct sockaddr*>(buf.data()), &len,
                SOCK_NONBLOCK | SOCK_CLOEXEC);
            break;
        }
        case co_excutor::CO_EVENT::CONNECT: {
            // buf 中存放的是 sockaddr 结构体数据
            io_uring_prep_connect(sqe, fd,
                reinterpret_cast<const sockaddr*>(buf.data()),
                static_cast<socklen_t>(buf.size()));
            break;
        }
        }

        auto* raw = completion.release();
        ui.inflight_cbs.insert(raw);
        io_uring_sqe_set_data(sqe, raw);
        io_uring_submit(&ui.ring);
    }

  private:
    struct uring_instance {
        struct io_uring ring{};
        std::mutex sq_mutex;
        std::set<io_callback_t*> inflight_cbs;
    };

    void event_loop(size_t index) {
        auto& ui = *urings_[index];

        while(running_) {
            struct io_uring_cqe* cqe = nullptr;

            struct __kernel_timespec ts{};
            ts.tv_sec  = 0;
            ts.tv_nsec = 100'000'000; // 100ms

            int ret = io_uring_wait_cqe_timeout(&ui.ring, &cqe, &ts);

            if(ret < 0) {
                if(ret == -ETIME || ret == -EINTR) continue;
                log::erro("excutor_uring: wait_cqe error on [{}]: {}", index,
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

    std::vector<std::unique_ptr<uring_instance>> urings_;
    std::vector<std::thread> loop_threads_;
    std::atomic<bool> running_{true};
};

static excutor_uring_impl& uring_impl() {
    static excutor_uring_impl instance;
    return instance;
}

void excutor_uring::async_io(CO_EVENT event, int fd,
    std::vector<unsigned char>& buf, io_callback_t cb, ssize_t len) {
    uring_impl().async_io(event, fd, buf, std::move(cb), len);
}
