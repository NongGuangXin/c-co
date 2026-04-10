#include "async.h"
#include "co_excutor.h"
#include "log.h"
#include "task.h"

#include <csignal>
#include <cstdlib>
#include <string>

static const std::string http_response = "HTTP/1.1 200 OK\r\n"
                                         "Content-Type: text/plain\r\n"
                                         "Content-Length: 13\r\n"
                                         "Connection: close\r\n"
                                         "\r\n"
                                         "Hello, World!";

static const std::vector<unsigned char> response_bytes(
    http_response.begin(), http_response.end());

task<int> handle_client(connection conn) {
    std::vector<unsigned char> buffer(4096);

    // Read the request (just consume it)
    auto read_result = co_await conn.co_read(buffer);
    if(!read_result.has_value() || read_result.value() == 0) { co_return -1; }

    // Write the HTTP response
    auto write_result = co_await conn.co_write(response_bytes);
    if(!write_result.has_value()) {
        log::erro("write error:{}", write_result.error());
    }

    co_return 0;
}

task<int> server(acceptor& ac) {
    log::info("Http server started, waiting for connections...");
    log::set_level(log::Level::WARN);

    while(true) {
        connection conn = co_await ac.co_accept();
        if(!conn) {
            log::erro("Accept error");
            continue;
        }

        log::info("New connection accepted");

        // 分离协程，不阻塞调用者
        co_excutor::detach(handle_client(conn));
    }
}

void signal_handler(int sig) {
    log::erro("Received signal: {}", sig);
    std::exit(EXIT_SUCCESS);
}

int main() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    std::signal(SIGHUP, signal_handler);

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
