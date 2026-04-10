#include "co_excutor.h"

void co_excutor::execute(task_t&& task) {
    pool.execute(std::move(task));
}

co_excutor& co_excutor::instance() {
    static excutor_epoll instance;
    // static excutor_uring instance;
    return instance;
}
