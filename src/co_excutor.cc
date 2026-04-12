#include "co_excutor.h"
#include "log.h"
#include <csignal>
#include <mutex>

static void signal_handler(int sig) {
    log::erro("Received signal: {}", sig);
    co_excutor::shutdown();
    std::exit(EXIT_SUCCESS);
}

void co_excutor::shutdown() {
    // 1. Stop event loop threads first (no more callbacks will fire)
    instance().stop();
    // 2. Now safe to destroy all suspended detached coroutine frames
    detached_registry::instance().destroy_all();
}

static void init_signal() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    std::signal(SIGHUP, signal_handler);
}

void co_excutor::execute(task_t&& task) {
    pool.execute(std::move(task));
}

co_excutor& co_excutor::instance() {
    static std::once_flag once;
    std::call_once(once, init_signal);

#if defined(USE_URING)
    static excutor_uring instance;
#else
    static excutor_epoll instance;
#endif
    return instance;
}

void bind_thread_to_cpu(std::thread& t, int cpu_id) {
    static unsigned int num_cpus = std::thread::hardware_concurrency();
    cpu_id                       = cpu_id % num_cpus;

    pthread_t pthread = t.native_handle();

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);

    int rc = pthread_setaffinity_np(pthread, sizeof(cpu_set_t), &cpuset);
    if(rc != 0) { log::erro("pthread_setaffinity_np error: {}", rc); }
    return;
}
