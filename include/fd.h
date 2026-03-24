#pragma once

#include <memory>

class FileDescriptor {
  public:
    FileDescriptor() = default;
    FileDescriptor(int fd);
    ~FileDescriptor() = default;

    FileDescriptor(const FileDescriptor&) noexcept            = default;
    FileDescriptor& operator=(const FileDescriptor&) noexcept = default;

    FileDescriptor(FileDescriptor&&) noexcept            = default;
    FileDescriptor& operator=(FileDescriptor&&) noexcept = default;

  public:
    int      handle() const noexcept;
    explicit operator bool() const noexcept;
    bool     operator==(const FileDescriptor& other) const noexcept;

  private:
    std::shared_ptr<int> fd_{nullptr};
};
