#include "async.h"
#include "log.h"
#include "task.h"
#include <cstddef>
#include <cstring>
#include <random>

std::vector<unsigned char> generate_random_ascii(size_t count) {
    std::vector<unsigned char> result;
    if(count > result.max_size()) {
        throw std::length_error("count exceeds vector<char> maximum capacity");
    }
    if(count == 0) {
        return result; // 快速返回空vector
    }
    // 2. 预分配空间：避免循环中多次扩容，提升性能
    result.reserve(count);
    // 3. 初始化随机数生成器（优先用非确定性随机设备，否则回退到高分辨率时间戳）
    std::random_device rd;
    std::mt19937 rng; // 梅森旋转算法，性能好、周期长
    if(rd.entropy() > 0) {
        // 有可用熵源（大多数现代桌面/服务器平台支持）
        rng.seed(rd());
    } else {
        // 无可用熵源（旧编译器/嵌入式），回退到时间戳
        auto now = std::chrono::high_resolution_clock::now();
        rng.seed(static_cast<unsigned long>(now.time_since_epoch().count()));
    }
    // 4. 均匀分布器：限定范围为ASCII可见字符 [32, 126]
    std::uniform_int_distribution<int> dist(32, 126);
    // 5. 循环生成字符
    for(size_t i = 0; i < count; ++i) {
        result.push_back(static_cast<unsigned char>(dist(rng)));
    }
    return result;
}

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
        std::vector<unsigned char> send_data = generate_random_ascii(i);

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

        if(buffer != send_data) {
            log::erro("Error: Data mismatch");
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
