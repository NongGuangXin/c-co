// pingpong_client - echo throughput benchmark
// Usage: pingpong_client [-p port] [-t threads] [-c connections] [-d duration]
// [-s size]

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

struct Config {
    int port         = 9999;
    int threads      = 4;
    int connections  = 1000;
    int duration_sec = 10;
    int msg_size     = 4096;
};

struct ThreadStats {
    long long read_count = 0;
    long long read_bytes = 0;
};

static std::atomic<bool> g_running{true};

static int make_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int connect_to(const char* host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if(fd < 0) return -1;

    int yes = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    make_nonblock(fd);

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, host, &addr.sin_addr);

    int ret = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if(ret < 0 && errno != EINPROGRESS) {
        close(fd);
        return -1;
    }
    return fd;
}

static void thread_func(
    const Config& cfg, int conns_per_thread, ThreadStats& stats) {
    int epfd = epoll_create1(EPOLL_CLOEXEC);
    if(epfd < 0) {
        perror("epoll_create1");
        return;
    }

    std::vector<unsigned char> msg(static_cast<size_t>(cfg.msg_size), 'A');
    std::vector<unsigned char> buf(static_cast<size_t>(cfg.msg_size * 2));

    // Per-fd write tracking: how many bytes still need to be sent for initial
    // kick
    struct ConnState {
        int fd = -1;
        // nothing else needed for simple pingpong
    };

    std::vector<ConnState> conns(static_cast<size_t>(conns_per_thread));
    int connected = 0;

    for(int i = 0; i < conns_per_thread; i++) {
        int fd = connect_to("127.0.0.1", cfg.port);
        if(fd < 0) continue;
        conns[static_cast<size_t>(i)].fd = fd;

        struct epoll_event ev{};
        ev.events  = EPOLLOUT; // wait for connect completion
        ev.data.fd = fd;
        epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
        connected++;
    }

    if(connected == 0) {
        close(epfd);
        return;
    }

    constexpr int MAX_EVENTS = 256;
    struct epoll_event events[MAX_EVENTS];

    while(g_running.load(std::memory_order_relaxed)) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, 10);
        for(int i = 0; i < n; i++) {
            int fd      = events[i].data.fd;
            uint32_t ev = events[i].events;

            if(ev & (EPOLLERR | EPOLLHUP)) {
                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
                continue;
            }

            if(ev & EPOLLOUT) {
                // Connected or writable: send message
                ssize_t nw = ::write(fd, msg.data(), msg.size());
                if(nw <= 0) {
                    if(errno != EAGAIN && errno != EWOULDBLOCK) {
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                    }
                    continue;
                }
                // Switch to reading
                struct epoll_event rev{};
                rev.events  = EPOLLIN;
                rev.data.fd = fd;
                epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &rev);
            }

            if(ev & EPOLLIN) {
                ssize_t nr = ::read(fd, buf.data(), buf.size());
                if(nr <= 0) {
                    if(nr == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                    }
                    continue;
                }
                stats.read_count++;
                stats.read_bytes += nr;

                // Ping back
                ssize_t nw = ::write(fd, msg.data(), msg.size());
                if(nw <= 0) {
                    if(errno != EAGAIN && errno != EWOULDBLOCK) {
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                    }
                    continue;
                }
                // Stay in EPOLLIN - level triggered so we'll get notified again
            }
        }
    }

    // Cleanup
    for(auto& c: conns) {
        if(c.fd >= 0) close(c.fd);
    }
    close(epfd);
}

int main(int argc, char* argv[]) {
    signal(SIGPIPE, SIG_IGN);

    Config cfg;

    for(int i = 1; i < argc; i++) {
        if(strcmp(argv[i], "-p") == 0 && i + 1 < argc)
            cfg.port = atoi(argv[++i]);
        else if(strcmp(argv[i], "-t") == 0 && i + 1 < argc)
            cfg.threads = atoi(argv[++i]);
        else if(strcmp(argv[i], "-c") == 0 && i + 1 < argc)
            cfg.connections = atoi(argv[++i]);
        else if(strcmp(argv[i], "-d") == 0 && i + 1 < argc)
            cfg.duration_sec = atoi(argv[++i]);
        else if(strcmp(argv[i], "-s") == 0 && i + 1 < argc)
            cfg.msg_size = atoi(argv[++i]);
        else {
            fprintf(stderr,
                "Usage: %s [-p port] [-t threads] [-c connections] [-d duration] [-s size]\n",
                argv[0]);
            return 1;
        }
    }

    printf("Running %ds test @ 127.0.0.1:%d\n", cfg.duration_sec, cfg.port);
    printf("%d threads and %d connections, send %d bytes each time\n",
        cfg.threads, cfg.connections, cfg.msg_size);

    int conns_per_thread = cfg.connections / cfg.threads;
    std::vector<ThreadStats> all_stats(static_cast<size_t>(cfg.threads));
    std::vector<std::thread> threads;

    auto start = std::chrono::steady_clock::now();

    for(int i = 0; i < cfg.threads; i++) {
        int extra = (i < cfg.connections % cfg.threads) ? 1 : 0;
        threads.emplace_back(thread_func, std::cref(cfg),
            conns_per_thread + extra,
            std::ref(all_stats[static_cast<size_t>(i)]));
    }

    std::this_thread::sleep_for(std::chrono::seconds(cfg.duration_sec));
    g_running.store(false, std::memory_order_relaxed);

    for(auto& t: threads) t.join();

    auto end       = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    long long total_count = 0, total_bytes = 0;
    for(auto& s: all_stats) {
        total_count += s.read_count;
        total_bytes += s.read_bytes;
    }

    printf("total readcount=%lld readbytes=%lld\n", total_count, total_bytes);
    printf("throughput = %lld MB/s\n",
        static_cast<long long>(
            static_cast<double>(total_bytes) / elapsed / 1024.0 / 1024.0));

    return 0;
}
