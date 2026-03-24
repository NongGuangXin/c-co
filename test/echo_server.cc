#include "log.h"
#include "async.h"
#include "task.h"

task<int> handle_client2(connection conn) {
    std::vector<unsigned char> buffer(4096);

    while(true) {
        auto read_result = co_await conn.co_read(buffer);

        if(!read_result.has_value()) {
            log::erro("read error:{}", read_result.error());
            break;
        }

        size_t bytes_read = read_result.value();
        if(bytes_read == 0) {
            log::info("client disconnected\n");
            break;
        }
        log::info("read {} byte from client", bytes_read);

        std::vector<unsigned char> response(
            buffer.begin(), buffer.begin() + bytes_read);
        auto write_result = co_await conn.co_write(response);

        if(!write_result.has_value()) {
            log::erro("write error:{}", write_result.error());
            break;
        }

        log::info("Echoed {} byte to client\n", bytes_read);
    }

    co_return 0;
}

task<int> server2(acceptor& ac) {
    log::info("Echo server started, waiting for connections...");

    while(true) {
        connection conn = co_await ac.co_accept();
        if(!conn) {
            log::erro("Accept error");
            continue;
        }

        log::info("New connection accepted");

        co_await handle_client2(conn);
    }
}

int main() {
    log::set_level(log::Level::INFO);
    log::info("{} start", __func__);

    acceptor ac = co_listen(9999);
    if(!ac) {
        log::erro("listen error");
        return -1;
    }

    sync_wait(server2(ac));

    log::info("{} exsit", __func__);
    return 0;
}
