#pragma once
#include <expected>
#include <vector>
#include "fd.h"
#include "task.h"

class connection {
  private:
  public:
    connection(FileDescriptor fd): fd_(fd) { }

    task<std::expected<size_t, int>> co_read(std::vector<unsigned char>& buf);

    task<std::expected<size_t, int>> co_write(
        const std::vector<unsigned char>& buf);

    explicit operator bool() const noexcept;

  private:
    FileDescriptor fd_;
    struct read_awaitable;
    struct write_awaitable;

    int handle();
};

class acceptor {
  private:
    struct accept_awaitable;

  public:
    acceptor(FileDescriptor fd): fd_(fd) { }
    explicit operator bool() const noexcept;

    task<connection> co_accept();

  private:
    FileDescriptor fd_;

    int handle();
};

acceptor co_listen(int port);

task<connection> co_connect(int port);
