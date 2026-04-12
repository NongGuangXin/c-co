#include "async.h"
#include "co_excutor.h"
#include "log.h"
#include "task.h"
#include <cstdlib>

task<int> handle_client(connection conn) {
    constexpr size_t BUF_SIZE = 65536;
    std::vector<unsigned char> buffer(BUF_SIZE);

    while(true) {
        buffer.resize(BUF_SIZE);
        auto read_result = co_await conn.co_read(buffer);
        if(!read_result.has_value()) { break; }

        size_t bytes_read = read_result.value();
        if(bytes_read == 0) { break; }

        // Resize to exact read size so co_write sends only valid data
        // This avoids allocating a new vector - resize doesn't free memory
        buffer.resize(bytes_read);
        auto write_result = co_await conn.co_write(buffer);
        if(!write_result.has_value()) { break; }
    }

    co_return 0;
}

task<int> server(acceptor& ac) {
    log::info("Echo server started, waiting for connections...");
    log::flush();
    log::set_level(log::Level::ERRO);

    while(true) {
        connection conn = co_await ac.co_accept();
        if(!conn) {
            log::erro("Accept error");
            break;
        }

        log::info("New connection accepted");

        // 分离协程，不阻塞调用者
        co_excutor::detach(handle_client(conn));
    }
    co_return 0;
}

int main() {
    log::set_level(log::Level::INFO);
    log::info("{} start", __func__);

    acceptor ac = co_listen(9999);
    if(!ac) {
        log::erro("listen error");
        return -1;
    }

    co_excutor::sync_wait(server(ac)); // 这里等待协程完成

    log::info("{} exsit", __func__);
    return 0;
}
