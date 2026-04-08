#include "async.h"
#include "log.h"
#include "task.h"
#include <cstddef>
#include <cstring>

task<int> echo_client(int port) {
    log::info("Connecting to server on port {}...", port);

    connection conn = co_await co_connect(port);
    if(!conn) {
        log::erro("Failed to connect to server");
        co_return -1;
    }

    log::info("Connected to server successfully");

    for(size_t i = 1; i < 8 * 1024 * 1024 + 99; i *= 2) {
        // 发送消息
        std::vector<unsigned char> send_data(i, 'A');

        auto write_result = co_await conn.co_write(send_data);
        if(!write_result.has_value()) {
            log::erro("{} bytes write error: {}", i, write_result.error());
            break;
        }

        // 接收回显
        std::vector<unsigned char> buffer(i);

        auto read_result = co_await conn.co_read_until(buffer);
        if(!read_result.has_value()) {
            log::erro("{} read error: {}", i, read_result.error());
            break;
        }

        size_t bytes_read = read_result.value();
        if(bytes_read == 0) {
            log::info("Server closed connection");
            break;
        }

        if(bytes_read != i) {
            log::erro("Error: Received {} bytes, expected {}", bytes_read, i);
            break;
        }

        log::info("Echoed {} bytes successfully", i);
        // std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }

    log::info("Client finished");
    co_return 0;
}

int main() {
    log::set_level(log::Level::INFO);
    log::info("Echo client starting...");

    excutor::sync_wait(echo_client(9999));

    log::info("Echo client exited");
    return 0;
}
