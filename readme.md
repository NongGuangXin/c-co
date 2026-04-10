# C++ Coroutine 异步网络库

这是一个基于 C++ 协程的异步网络库（仅供学习），支持 **epoll** 和 **io_uring** 双后端。

## 特性

- **C++23 协程支持**：利用 `co_await`、`co_return` 等关键字实现简洁的异步代码
- **双 I/O 后端**：同时支持 epoll 和 io_uring，编译时通过 `USE_URING` 宏切换
- **fd 分片并行**：N 个事件循环线程（每 CPU 一个），每个拥有独立的 epoll/uring 实例，fd 按 `fd % N` 路由
- **对称转移（Symmetric Transfer）**：协程链式调用零开销，避免栈溢出
- **错误处理**：使用 `std::expected<size_t, int>` 进行 I/O 错误处理，无异常路径
- **I/O 与执行分离**：事件循环线程负责 I/O 完成检测，协程恢复在线程池中执行
- **自销毁协程帧**：`detach()` 和 `sync_wait()` 使用自销毁机制防止内存泄漏
- **分级日志系统**：支持 `source_location` 自动标注文件/行号，可选 spdlog 后端
- **简单**：只实现必须的功能

## 项目结构

```
.
├── include/                # 头文件目录
│   ├── task.h              # 协程任务类型 task<T>
│   ├── async.h             # 异步网络操作（connection、acceptor、co_connect、co_listen）
│   ├── co_excutor.h        # 事件循环执行器（抽象基类 + epoll/uring 子类）
│   ├── file_descriptor.h   # RAII 文件描述符封装（引用计数）
│   ├── log.h               # 分级日志系统
│   └── thrdpool.h          # 线程池
├── src/                    # 源文件目录
│   ├── co_excutor.cc       # 执行器工厂、信号处理、CPU 亲和性
│   ├── excutor_epoll.cc    # epoll 后端（多实例、fd 分片、EPOLLET+ONESHOT）
│   ├── excutor_iouring.cc  # io_uring 后端（多实例、fd 分片、队列深度 2048）
│   ├── async.cc            # Awaitable 实现（read、write、accept、connect）
│   ├── log.cc              # 日志实现
│   └── thrdpool.cc         # 线程池实现
├── test/                   # 测试和示例程序
│   ├── echo_server.cc      # 协程回显服务器
│   ├── echo_client.cc      # 协程回显客户端
│   ├── echo_client_buf.cc  # 缓冲回显客户端（精确读取、大负载测试）
│   ├── echo_client_mt.cc   # 多线程压力测试客户端
│   ├── http_server.cc      # 最小 HTTP 服务器（适用于 ab 压测）
│   ├── pingpong_client.cc  # 高吞吐 pingpong 基准测试客户端
│   ├── task_test.cc        # task<T> 协程机制测试
│   └── thrd_test.cc        # 线程池测试
├── CMakeLists.txt          # CMake 构建配置（双后端）
├── Makefile                # Makefile 封装
├── .clang-format           # 代码格式化配置
└── code-format.py          # 批量格式化工具
```

## 构建要求

- **编译器**：GCC 13+ 或 Clang 16+（需支持 C++23 协程、`std::expected`、`std::format`）
- **构建工具**：CMake 3.21+ 和 Ninja
- **操作系统**：Linux（需要 epoll 支持；io_uring 后端额外需要 liburing）
- **依赖**：pthread；io_uring 后端需要 liburing

## 构建步骤

### 使用 Makefile（推荐）

```bash
# 构建项目（Release 模式）
make build

# 清理构建
make clean
```

### 使用 CMake 直接构建

```bash
mkdir -p build
cmake -S . -B build -G "Ninja"
cmake --build build/
```

### 构建产物

构建会同时生成 epoll 和 io_uring 两套产物：

| 后端 | 共享库 | 可执行文件目录 | 额外依赖 |
|------|--------|--------------|---------|
| epoll | `libc++co_epoll.so` | `build/bin/epoll/` | pthread |
| io_uring | `libc++co_uring.so` | `build/bin/uring/` | liburing |

每个测试程序都会编译两份，分别链接不同后端。

### Debug 模式

Debug 构建会启用 AddressSanitizer、LeakSanitizer 和 UndefinedBehaviorSanitizer：

```bash
cmake -S . -B build -G "Ninja" -DCMAKE_BUILD_TYPE=Debug
```

## 运行示例

### 回显服务器/客户端

```bash
# 启动 epoll 后端的回显服务器（监听端口 9999）
./build/bin/epoll/echo_server

# 启动 io_uring 后端的回显服务器
./build/bin/uring/echo_server

# 运行回显客户端
./build/bin/epoll/echo_client
```

### HTTP 服务器压测

```bash
./build/bin/epoll/http_server

ab -c 100 -n 100000 http://127.0.0.1:9999/
```

### Pingpong 基准测试

```bash
# 先启动 echo_server，然后运行 pingpong 测试
./build/bin/epoll/pingpong_client

# 完整参数示例
./build/bin/epoll/pingpong_client -p 9999 -t 4 -c 100 -d 10 -s 1024
# 参数：-p 端口 -t 线程数 -c 连接数 -d 持续时间(秒) -s 消息大小(字节)
```

### 其他测试

```bash
# 协程任务测试
./build/bin/epoll/task_test

# 线程池测试
./build/bin/epoll/thrd_test

# 缓冲客户端（指数增长负载测试，1 字节到 8MB，包括回显数据准确性）
./build/bin/epoll/echo_client_buf

# 多线程压力测试
./build/bin/epoll/echo_client_mt
```

## 核心组件说明

### 1. task\<T\> - 协程任务

[`task<T>`](include/task.h) 是协程的返回类型，表示一个惰性异步计算：

```cpp
task<int> my_coroutine() {
    co_await some_async_operation();
    co_return 42;
}
```

**关键特性**：
- **完全惰性**：`initial_suspend` 返回 `suspend_always`，创建即挂起
- **对称转移**：`final_awaiter` 直接返回 continuation handle，零开销链式调用
- **自销毁支持**：`self_destroy_` 标志使帧在 `final_suspend` 时自动析构
- **Move-only RAII**：析构时自动销毁协程帧（除非已 `release()`）
- 支持 `co_await` 等待其他任务、`co_return` 返回结果

### 2. connection - 连接管理

[`connection`](include/async.h) 封装了 TCP 连接的异步操作：

```cpp
class connection {
public:
    explicit operator bool() const noexcept;

    // 单次读取，返回可用数据
    read_awaitable co_read(std::vector<unsigned char>& buf);

    // 精确读取 buf.size() 字节（或直到 EOF/错误）
    read_until_awaitable co_read_until(std::vector<unsigned char>& buf);

    // 写入所有数据，自动处理短写
    write_awaitable co_write(const std::vector<unsigned char>& buf);
};
```

所有操作返回 `std::expected<size_t, int>`：成功时为字节数（0 表示 EOF），失败时为 errno。read操作中，对端关闭或出错时，buf中保存有关闭前或出错前读取到的数据。

### 3. acceptor - 监听器

[`acceptor`](include/async.h) 用于监听和接受新连接：

```cpp
class acceptor {
public:
    operator bool() const;
    accept_awaitable co_accept();  // 返回新的 connection
};

acceptor ac = co_listen(9999);     // 创建监听（SO_REUSEADDR | SO_REUSEPORT）
connection conn = co_connect(9999); // 非阻塞连接到 localhost
```

### 4. co_excutor - 执行器

[`co_excutor`](include/co_excutor.h) 是单例模式的事件循环和任务调度器，通过编译时 `USE_URING` 宏选择后端实现：

```cpp
class co_excutor {
public:
    enum CO_EVENT { READ = 0x1, WRITE = 0x2, ACCEPT = 0x4, CONNECT = 0x8 };

    static co_excutor& instance();           // 单例（epoll 或 uring）
    void execute(task_t&& task);             // 提交任务到线程池
    std::future<R> submit(F&& f, Args&&...); // 提交任意可调用对象到线程池
    static T sync_wait(task<T>&& t);         // 阻塞等待协程完成
    static void detach(task<T>&& t);         // 启动并分离协程

    // 纯虚函数，由具体后端实现
    virtual void async_io(co_event ev, int fd, std::vector<unsigned char>& buf,
                          io_callback_t cb, ssize_t len = -1) = 0;
};
```

### 5. 日志系统

[`log`](include/log.h) 提供分级日志功能，使用 `std::format` 格式化，`source_location` 自动标注调用位置：

```cpp
class log {
public:
    enum class Level : unsigned int { DBUG = 0, INFO, WARN, ERRO, STOP };

    static void dbug(Fmt fmt, const auto&... args);  // 包含文件:行号
    static void info(Fmt fmt, const auto&... args);
    static void warn(Fmt fmt, const auto&... args);
    static void erro(Fmt fmt, const auto&... args);   // 包含文件:行号

    static void set_level(Level level);
};
```

## 架构概览

```
用户协程代码 (test/*.cc)
        │
        │ co_await conn.co_read() / co_write() / co_accept() / co_connect()
        ▼
   Awaitable 结构体 (async.h / async.cc)
        │
        │ 调用 co_excutor::instance().async_io(event, fd, buf, callback)
        ▼
   co_excutor 单例（编译时选择后端）
        │
        ├── excutor_epoll (N 个 epoll_instance，fd 分片，EPOLLET+ONESHOT)
        │   └── 事件循环线程 → readall/do_write/do_accept/do_connect → callback
        │
        └── excutor_uring (N 个 uring_instance，fd 分片，队列深度 2048)
            └── 事件循环线程 → CQE 处理 → finalize_read → callback
        │
        │ callback 调用 coroutine_handle.resume()
        ▼
     协程恢复
```

**关键设计**：
1. **后端多态**：通过虚函数 `async_io()` 实现，编译时选择 epoll 或 io_uring
2. **fd 分片并行**：N 个事件循环线程各自独立，fd 按 `fd % N` 路由，无锁竞争
3. **I/O 完成与执行分离**：事件循环线程检测就绪并执行 I/O，通过回调恢复协程
4. **对称转移**：`await_suspend` 返回 `coroutine_handle` 而非 `void`，避免栈深度增长

## 编译选项

| 模式 | 编译选项 |
|------|---------|
| Release（默认） | `-std=c++23 -Wall -Wextra` |
| Debug | `-std=c++23 -g -O1 -fno-omit-frame-pointer -Wall -Wextra -fsanitize=address,leak,undefined` |

## 注意事项

1. **C++23 要求**：需要编译器支持协程、`std::expected`、`std::format`
2. **仅 Linux**：依赖 epoll / io_uring 系统调用
3. **多线程架构**：N 个事件循环线程 + 线程池工作线程
4. **`sync_wait`**：会阻塞当前线程直到协程完成，不要在协程内调用

## TODO

- [x] 多 epoll 线程和多 IO 线程
- [x] 更详尽的测试
- [x] 基础设施和 echo Demo
- [x] io_uring 后端支持
- [ ] 定时器支持
- [ ] UDP 支持

## 许可证

MIT License
