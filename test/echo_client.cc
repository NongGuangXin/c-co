#include "async.h"
#include "co_excutor.h"
#include "log.h"
#include "task.h"
#include <cstring>
#include <string_view>

task<int> echo_client(int port) {
    log::info("Connecting to server on port {}...", port);

    connection conn = co_await co_connect(port);
    if(!conn) {
        log::erro("Failed to connect to server");
        co_return -1;
    }

    log::info("Connected to server successfully");

    // 发送消息
    std::string_view message = "Hello, Echo Server!";
    std::vector<unsigned char> send_data(message.begin(), message.end());

    auto write_result = co_await conn.co_write(send_data);
    if(!write_result.has_value()) {
        log::erro("Write error: {}", write_result.error());
        co_return -1;
    }

    log::info("Sent {} bytes to server: {}", write_result.value(), message);

    // 接收回显
    std::vector<unsigned char> buffer(4096);
    auto read_result = co_await conn.co_read(buffer);

    if(!read_result.has_value()) {
        log::erro("Read error: {}", read_result.error());
        co_return -1;
    }

    size_t bytes_read = read_result.value();
    if(bytes_read == 0) {
        log::info("Server closed connection");
        co_return 0;
    }

    std::string response(buffer.begin(), buffer.begin() + bytes_read);
    log::info("Received {} bytes from server: {}", bytes_read, response);

    // 发送多条消息测试
    for(int i = 0; i < 3; i++) {
        std::string msg = "Test message #" + std::to_string(i + 1);
        std::vector<unsigned char> send_buf(msg.begin(), msg.end());

        auto wr = co_await conn.co_write(send_buf);
        if(!wr.has_value()) {
            log::erro("Write error: {}", wr.error());
            break;
        }
        log::info("Sent: {}", msg);

        std::vector<unsigned char> recv_buf(4096);
        auto rr = co_await conn.co_read(recv_buf);
        if(!rr.has_value()) {
            log::erro("Read error: {}", rr.error());
            break;
        }

        if(rr.value() == 0) {
            log::info("Server closed connection");
            break;
        }

        std::string echo(recv_buf.begin(), recv_buf.begin() + rr.value());
        log::info("Received echo: {}", echo);
    }

    log::info("Client finished");
    co_return 0;
}

int main() {
    log::set_level(log::Level::INFO);
    log::info("Echo client starting...");

    co_excutor::sync_wait(echo_client(9999));

    log::info("Echo client exited");
    return 0;
}
