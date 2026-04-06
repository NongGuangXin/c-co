#include "log.h"
#include "task.h"
#include "excutor.h"

task<int> add(int a, int b) {
    co_return a + b;
}

task<int> nested() {
    int a = co_await add(1, 2);
    int b = co_await add(3, 4);
    co_return a + b;
}

task<void> void_task() {
    log::info("void_task running");
    co_return;
}

int main() {
    log::set_level(log::Level::DBUG);

    log::info("=== task test ===");

    // Test simple task
    int result = excutor::sync_wait(add(10, 20));
    log::info("add(10, 20) = {}", result);

    // Test nested task
    int nested_result = excutor::sync_wait(nested());
    log::info("nested() = {}", nested_result);

    // Test void task
    excutor::sync_wait(void_task());
    log::info("void_task completed");

    log::info("=== all tests passed ===");
    return 0;
}
