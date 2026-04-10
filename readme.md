# C++ Coroutine 异步网络库

这是一个基于 C++ 协程（Coroutine）的异步网络库（仅供学习）

## 特性

- **C++23 协程支持**：利用`co_await`、`co_return`等关键字实现简洁的异步代码
- **epoll 事件驱动**：基于 Linux epoll 实现网络I/O多路复用
- **错误处理**：使用 `std::expected` 进行错误处理
- **IO和事件分离**：epoll事件触发和IO操作线程分离

## 项目结构

```
.
├── include/          # 头文件目录
│   ├── async.h       # 异步网络操作（connection、acceptor）
│   ├── co_excutor.h     # 任务执行器和事件循环
│   ├── log.h         # 日志系统
│   └── task.h        # 协程任务类型
├── src/              # 源文件目录
│   ├── async.cc      # 异步网络实现
│   ├── co_excutor.cc    # 执行器实现
│   └── log.cc        # 日志实现
├── test/             # 测试程序
│   ├── echo_server.cc  # 回显服务器示例
│   ├── echo_client.cc  # 回显客户端示例
│   ├── task_test.cc    # 任务测试
│   └── thrdtest.cc     # 线程测试
├── CMakeLists.txt    # CMake 构建配置
└── Makefile          # Makefile 封装
```

## 构建要求

- **编译器**：支持 C++23 的编译器（GCC 13+ 或 Clang 16+）
- **构建工具**：CMake 3.21+ 和 Ninja
- **操作系统**：Linux（需要 epoll 支持）

## 构建步骤

### 使用 Makefile（推荐）

```bash
# 构建项目
make build

# 清理构建
make clean
```

### 使用 CMake 直接构建

```bash
# 创建构建目录
mkdir -p build

# 配置项目
cmake -S . -B build -G "Ninja"

# 编译
cmake --build build/
```

## 运行示例

### 1. 启动回显服务器

```bash
./build/echo_server
```

服务器将在端口 9999 监听连接。

### 2. 运行回显客户端

```bash
./build/echo_client
```

客户端将连接到服务器，发送测试消息并接收回显。

### 3. 运行其他测试

```bash
# 任务测试
./build/task_test

./build/echo_client_mt

# for i in $(seq 1 50); do timeout 2 ./build/echo_client_buf 2>&1 > /dev/null; done && echo "50次运行完成，无卡死"

ab -c 100 -n 100000 http://127.0.0.1:9999/
```

## 核心组件说明

### 1. task<T> - 协程任务

[`task<T>`](include/task.h)是协程的返回类型，表示一个异步计算：

```cpp
task<int> my_coroutine() {
    co_await some_async_operation();
    co_return 42;
}
```

**关键特性**：
- 支持 `co_await` 等待其他任务完成
- 支持 `co_return` 返回结果
- 通过 `sync_wait()` 同步等待任务完成

### 2. connection - 连接管理

[`connection`](include/async.h) 封装了TCP操作：

```cpp
class connection {
  public:
    explicit operator bool() const noexcept；

    read_awaitable co_read(std::vector<unsigned char>& buf);
    read_until_awaitable co_read_until(std::vector<unsigned char>& buf);
    write_awaitable co_write(const std::vector<unsigned char>& buf);
}

connect_awaitable co_connect(int port);
```

### 3. acceptor - 监听器

[`acceptor`](include/async.h) 用于监听和接受新连接：

```cpp
class acceptor {
  public:
    operator bool() const；
    accept_awaitable co_accept();
}

acceptor ac = co_listen(9999);   // 在端口9999监听
```

### 4. co_excutor - 执行器

[`co_excutor`](include/co_excutor.h) 是单例模式的事件循环和任务调度器：

- 管理 epoll 事件循环
- 调度协程任务的执行
- 处理I/O事件的挂起和恢复

```cpp
class co_excutor {
  public:
    using task_t = std::function<void()>;

    enum co_event {
        READ,
        WRITE,
    };

    static co_excutor& instance();

    void execute(task_t task);
    void register_event(const FileDescriptor& fd, co_event ev, task_t task);
    void unregister_event(const FileDescriptor& fd);
    std::future<R> submit(F&& f, Args&&... args);

    static T sync_wait(task<T>&& t);
    static void detach(task<T>&& t)
}
```

### 5. 日志系统

[`log`](include/log.h) 提供分级日志功能：

```cpp
class log {
  public:
    enum class Level : unsigned int { DBUG = 0, INFO, WARN, ERRO, STOP };

    static void dbug(Fmt fmt, const auto&... args);
    static void info(Fmt fmt, const auto&... args);
    static void warn(Fmt fmt, const auto&... args);
    static void erro(Fmt fmt, const auto&... args);

    static void set_level(Level level);
    static Level get_level();
}
```

## 工作原理

### 协程执行流程

1. **创建协程**：调用协程函数创建 `task<T>` 对象
2. **初始挂起**：协程在开始时自动挂起（`initial_suspend` 返回 `suspend_always`）
3. **调度执行**：通过 `co_excutor` 将协程加入执行队列
4. **I/O 挂起**：当遇到I/O操作时，协程挂起并注册到epoll
5. **事件唤醒**：当I/O就绪时，epoll触发回调通知执行器恢复协程
6. **完成通知**：协程完成后通过 `final_awaiter` 通知等待者

### 异步 I/O 机制

```
┌─────────────┐     co_await      ┌─────────────┐
│  协程代码    │ ────────────────> │  awaitable  │
└─────────────┘                   └──────┬──────┘
                                         │
                    ┌────────────────────┘
                    │ await_ready()?
                    ▼
            ┌───────────────┐
            │  I/O 就绪？    │
            └───────┬───────┘
            是 /    \ 否
              /      \
             ▼        ▼
    ┌──────────┐  ┌──────────────┐
    │直接执行   │  │注册到 epoll  │
    └──────────┘  │await_suspend()│
                  └──────┬───────┘
                         │
                         ▼
                  ┌──────────────┐
                  │ 等待 I/O 事件 │
                  └──────┬───────┘
                         │ 事件就绪
                         ▼
                  ┌──────────────┐
                  │ resume()     │
                  │ await_resume()│
                  └──────────────┘
```

### 核心类关系

```
┌─────────────────────────────────────────────────────────┐
│                        co_excutor                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │  thrdLoop   │  │   epoller   │  │  task queue     │  │
│  │ (线程池)     │ │ (epoll封装)  │  │ (任务队列)       │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────┘
                           ▲
                           │ 调度/挂起
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│    task<T>    │  │  connection   │  │   acceptor    │
│  (协程任务)    │  │  (TCP连接)     │  │  (监听接受器)  │
└───────────────┘  └───────────────┘  └───────────────┘
                           │                  │
             ┌───server────┴─────client──┐    │
             │                           │    │
             ▼                           ▼    ▼
   ┌─────────────────────────────────────────────────┐
   │              FileDescriptor                     │
   │              (文件描述符封装)                     │
   └─────────────────────────────────────────────────┘
```

## 示例代码

见`test`目录下测试代码

## 编译选项

项目默认使用以下编译选项：

- `-std=c++23`：C++23 标准
- `-g`：调试信息
- `-O1`：优化级别 1
- `-fno-omit-frame-pointer`：保留帧指针（便于调试）
- `-Wall -Wextra`：启用所有警告
- `-fsanitize=address,leak,undefined`：AddressSanitizer 内存检测

## 注意事项

1. **C++23 要求**：需要支持协程和expected
2. **Linux 系统**：使用了 Linux 特有的 epoll 机制
3. **多线程**：当前实现使用多epoll线程和多IO处理线程
4. **同步等待**：`sync_wait` 会阻塞当前线程直到协程完成

## TODO
- ☑ 多epoll线程和多IO线程
- ☑ 更详尽的测试
- ☑ 基础设施和echo Demo

## 许可证

MIT License
