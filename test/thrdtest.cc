#include "log.h"
#include "excutor.h"

int main() {
    log::set_level(log::Level::DBUG);
    log::info("{} start", __func__);
    excutor& ex = excutor::instance();

    for(size_t i = 0; i < 10; i++) {
        ex.execute([]() { log::info("excutor thrd test"); });
    }

    return 0;
}
