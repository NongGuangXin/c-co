#include "log.h"
#include "task.h"
#include <unistd.h>

task<int> add(int a, int b) {
    co_return a + b;
}

task<int> bar() {
    log::info("{} start", __func__);
    co_return co_await add(1, 4);
}

int main() {
    log::set_level(log::Level::DBUG);
    log::info("{} start", __func__);

    int c = sync_wait(add(1, 3));
    int r = sync_wait(bar());
    log::info("c:{}:{}", c, r);

    return 0;
}
