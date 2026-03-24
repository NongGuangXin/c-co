#pragma once
#include <cstddef>
#include <future>
#include <functional>

#include "fd.h"

enum co_event : size_t { READ = 0x01, WRITE = 0x02, EVENT_MAX };

using task_t = std::function<void()>;

class excutor {
  public:
    static excutor& instance() {
        static excutor ex;
        return ex;
    }

    excutor(const excutor&)            = delete;
    excutor& operator=(const excutor&) = delete;

    void execute(task_t task);
    void suspend(const FileDescriptor& fd, co_event ev, task_t task);

    template <typename F, typename... Args,
        typename R = std::invoke_result_t<F, Args...>>
    std::future<R> submit(F&& f, Args&&... args) {
        auto packaged = std::make_shared<std::packaged_task<R()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));

        excute([packaged]() { (*packaged)(); });
        return packaged->get_future();
    }

  private:
    excutor();
    ~excutor();

    class excutorImpl;
    std::unique_ptr<excutorImpl> impl_;
};

std::string thread_id();
