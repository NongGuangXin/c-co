#include "async.h"
#include "co_excutor.h"
#include "log.h"
#include "task.h"

#include <cstdlib>
#include <string>

static const std::string http_response_close = "HTTP/1.1 200 OK\r\n"
                                               "Content-Type: text/plain\r\n"
                                               "Content-Length: 13\r\n"
                                               "Connection: close\r\n"
                                               "\r\n"
                                               "Hello, World!";

static const std::string http_response_keepalive =
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 13\r\n"
    "Connection: keep-alive\r\n"
    "\r\n"
    "Hello, World!";

static const std::vector<unsigned char> response_close_bytes(
    http_response_close.begin(), http_response_close.end());

static const std::vector<unsigned char> response_keepalive_bytes(
    http_response_keepalive.begin(), http_response_keepalive.end());

task<int> handle_client(connection conn) {
    std::vector<unsigned char> buffer(1024);

    while(true) {
        // Read the request
        auto read_result = co_await conn.co_read(buffer);
        if(!read_result.has_value() || read_result.value() == 0) {
            co_return -1;
        }

        // Check if the request asks for keep-alive (simple heuristic)
        // ab sends "Connection: Keep-Alive" when using -k flag
        size_t bytes_read = read_result.value();
        std::string_view req(
            reinterpret_cast<const char*>(buffer.data()), bytes_read);

        bool keep_alive = (req.find("keep-alive") != std::string_view::npos) ||
                          (req.find("Keep-Alive") != std::string_view::npos) ||
                          (req.find("Keep-alive") != std::string_view::npos);

        // Write the HTTP response
        const auto& resp =
            keep_alive ? response_keepalive_bytes : response_close_bytes;
        auto write_result = co_await conn.co_write(resp);
        if(!write_result.has_value()) { co_return -1; }

        if(!keep_alive) { co_return 0; }
    }
}

task<int> server(acceptor& ac) {
    log::info("Http server started, waiting for connections...");
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
