#include "co_excutor.h"
#include "excutor.h"

void co_excutor::execute(task_t&& task) {
    excutor::instance().execute(std::forward<task_t>(task));
}

co_excutor& co_excutor::instance() {
    // static excutor_epoll instance;
    static excutor_uring instance;
    return instance;
}
