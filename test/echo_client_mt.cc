#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

// ------------------------------
// 配置参数（可修改）
// ------------------------------
constexpr const char* SERVER_IP             = "127.0.0.1"; // 服务器IP
constexpr uint16_t    SERVER_PORT           = 9999;        // 服务器端口
static int            NUM_CONNECTIONS       = 50;          // 并发连接数
constexpr int         NUM_MESSAGES_PER_CONN = 5;    // 每个连接发送的消息数
constexpr int         BUFFER_SIZE           = 1024; // 缓冲区大小
constexpr int         TIMEOUT_SEC           = 5;    // 读写超时时间（秒）

// ------------------------------
// 全局线程安全计数器（用于生成唯一消息ID）
// ------------------------------
std::atomic<uint64_t> g_message_counter{0};

// ------------------------------
// 工具函数：设置socket为非阻塞 + 超时
// ------------------------------
bool set_socket_nonblocking_and_timeout(int fd, int timeout_sec) {
    // 1. 设置非阻塞
    int flags = fcntl(fd, F_GETFL, 0);
    if(flags == -1) {
        perror("fcntl(F_GETFL) failed");
        return false;
    }
    if(fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl(F_SETFL) failed");
        return false;
    }

    // 2. 设置读写超时（select/poll兼容，非阻塞socket的epoll也可参考）
    struct timeval tv;
    tv.tv_sec  = timeout_sec;
    tv.tv_usec = 0;
    if(setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == -1) {
        perror("setsockopt(SO_RCVTIMEO) failed");
        return false;
    }
    if(setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == -1) {
        perror("setsockopt(SO_SNDTIMEO) failed");
        return false;
    }

    return true;
}

// ------------------------------
// 单个连接的工作函数
// ------------------------------
void echo_client_worker(int conn_id) {
    int  sock_fd   = -1;
    bool connected = false;

    // ------------------------------
    // 1. 创建socket
    // ------------------------------
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if(sock_fd == -1) {
        perror("[Worker] socket creation failed");
        return;
    }

    // ------------------------------
    // 2. 设置socket属性（非阻塞+超时）
    // ------------------------------
    if(!set_socket_nonblocking_and_timeout(sock_fd, TIMEOUT_SEC)) {
        close(sock_fd);
        return;
    }

    // ------------------------------
    // 3. 连接服务器（非阻塞connect，需用select/poll检查）
    // ------------------------------
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port   = htons(SERVER_PORT);
    if(inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr) <= 0) {
        perror("[Worker] invalid server IP");
        close(sock_fd);
        return;
    }

    int connect_ret =
        connect(sock_fd, (sockaddr*)&server_addr, sizeof(server_addr));
    if(connect_ret == -1) {
        if(errno != EINPROGRESS) {
            perror("[Worker] connect failed immediately");
            close(sock_fd);
            return;
        }

        // 非阻塞connect正在进行，用select检查是否可写（连接成功/失败）
        fd_set write_fds;
        FD_ZERO(&write_fds);
        FD_SET(sock_fd, &write_fds);
        struct timeval tv;
        tv.tv_sec  = TIMEOUT_SEC;
        tv.tv_usec = 0;

        int select_ret = select(sock_fd + 1, nullptr, &write_fds, nullptr, &tv);
        if(select_ret <= 0) {
            perror("[Worker] connect timeout or select failed");
            close(sock_fd);
            return;
        }

        // 检查socket是否真的连接成功（getsockopt获取SO_ERROR）
        int       error = 0;
        socklen_t len   = sizeof(error);
        if(getsockopt(sock_fd, SOL_SOCKET, SO_ERROR, &error, &len) == -1 ||
            error != 0) {
            std::cerr << "[Worker] connect failed: " << strerror(error)
                      << std::endl;
            close(sock_fd);
            return;
        }
    }

    connected = true;
    std::cout << "[Worker " << conn_id << "] connected to server successfully"
              << std::endl;

    // ------------------------------
    // 4. 发送+接收消息（循环NUM_MESSAGES_PER_CONN次）
    // ------------------------------
    char send_buf[BUFFER_SIZE];
    char recv_buf[BUFFER_SIZE];
    memset(send_buf, 0, BUFFER_SIZE);
    memset(recv_buf, 0, BUFFER_SIZE);

    for(int msg_idx = 0; msg_idx < NUM_MESSAGES_PER_CONN; ++msg_idx) {
        // 生成唯一消息
        uint64_t msg_id =
            g_message_counter.fetch_add(1, std::memory_order_relaxed);
        snprintf(send_buf, BUFFER_SIZE,
            "[Conn %d][Msg %llu] Hello from C++20 echo client!", conn_id,
            static_cast<unsigned long long>(msg_id));
        size_t send_len = strlen(send_buf);

        // ------------------------------
        // 发送消息（非阻塞write，需循环直到写完）
        // ------------------------------
        size_t total_sent = 0;
        while(total_sent < send_len) {
            ssize_t sent =
                write(sock_fd, send_buf + total_sent, send_len - total_sent);
            if(sent > 0) {
                total_sent += sent;
            } else if(sent == -1) {
                if(errno == EAGAIN || errno == EWOULDBLOCK) {
                    // 非阻塞发送缓冲区满，等待一下
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    continue;
                } else {
                    perror("[Worker] write failed");
                    goto cleanup;
                }
            } else {
                std::cerr << "[Worker] connection closed by server during send"
                          << std::endl;
                goto cleanup;
            }
        }
        std::cout << "[Worker " << conn_id << "] sent: " << send_buf
                  << std::endl;

        // ------------------------------
        // 接收消息（非阻塞read，需循环直到收完echo的消息）
        // ------------------------------
        size_t total_recv   = 0;
        bool   recv_success = false;
        while(total_recv < send_len) {
            ssize_t recv = read(
                sock_fd, recv_buf + total_recv, BUFFER_SIZE - 1 - total_recv);
            if(recv > 0) {
                total_recv += recv;
                recv_buf[total_recv] = '\0';
                if(total_recv == send_len) {
                    recv_success = true;
                    break;
                }
            } else if(recv == -1) {
                if(errno == EAGAIN || errno == EWOULDBLOCK) {
                    // 非阻塞接收缓冲区空，等待一下
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    continue;
                } else {
                    perror("[Worker] read failed");
                    goto cleanup;
                }
            } else {
                std::cerr << "[Worker] connection closed by server during recv"
                          << std::endl;
                goto cleanup;
            }
        }

        if(recv_success) {
            std::cout << "[Worker " << conn_id << "] received: " << recv_buf
                      << std::endl;
        } else {
            std::cerr << "[Worker] failed to receive complete echo message"
                      << std::endl;
        }

        // 清空缓冲区
        memset(send_buf, 0, BUFFER_SIZE);
        memset(recv_buf, 0, BUFFER_SIZE);
    }

cleanup:
    // ------------------------------
    // 5. 关闭socket
    // ------------------------------
    if(connected) {
        std::cout << "[Worker " << conn_id << "] disconnecting from server"
                  << std::endl;
    }
    if(sock_fd != -1) { close(sock_fd); }
}

// ------------------------------
// 主函数
// ------------------------------
int main(int argc, char* argv[]) {
    std::cout << "=== C++20 Multi-Threaded TCP Echo Client ===" << std::endl;
    std::cout << "Server: " << SERVER_IP << ":" << SERVER_PORT << std::endl;
    std::cout << "Concurrent connections: " << NUM_CONNECTIONS << std::endl;
    std::cout << "Messages per connection: " << NUM_MESSAGES_PER_CONN
              << std::endl;
    std::cout << "===============================================" << std::endl;

    if(argc == 2) { NUM_CONNECTIONS = std::stoi(argv[1]); }

    std::cout << "NUM_CONNECTIONS: " << NUM_CONNECTIONS << std::endl;
    std::cout << "NUM_MESSAGES_PER_CONN: " << NUM_MESSAGES_PER_CONN
              << std::endl;
    std::cout << "===============================================" << std::endl;

    // 创建并启动工作线程
    std::vector<std::thread> workers;
    workers.reserve(NUM_CONNECTIONS);
    for(int conn_id = 0; conn_id < NUM_CONNECTIONS; ++conn_id) {
        workers.emplace_back(echo_client_worker, conn_id);
    }

    // 等待所有工作线程完成
    for(auto& worker: workers) {
        if(worker.joinable()) { worker.join(); }
    }

    std::cout << "===============================================" << std::endl;
    std::cout << "All workers finished successfully!" << std::endl;
    return 0;
}
