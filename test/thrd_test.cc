#include "thrdpool.h"
#include "log.h"

std::atomic<int> count = 0;

void foo() {
    count.fetch_add(1);
    // log::info("foo:{}", count.load());
}

int main() {
    log::set_level(log::Level::DBUG);

    thrdpool pool(4);

    for(int i = 0; i < 100; i++) { pool.execute(foo); }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if(count.load() != 100) {
        log::erro("count is not 100 : {}", count.load());
        return 1;
    }
    return 0;
}
