#pragma once

#include <memory>
#include <unistd.h>

class FileDescriptor {
  public:
    FileDescriptor() = default;

    FileDescriptor(int fd):
        fd_(new int(fd), [](int* fd) {
            if(fd != nullptr && *fd >= 0) { ::close(*fd); }
            if(fd != nullptr) { delete fd; }
        }) { }

    FileDescriptor(const FileDescriptor&) noexcept            = default;
    FileDescriptor& operator=(const FileDescriptor&) noexcept = default;

    FileDescriptor(FileDescriptor&&) noexcept            = default;
    FileDescriptor& operator=(FileDescriptor&&) noexcept = default;

  public:
    int handle() const noexcept {
        return fd_ ? *fd_ : -1;
    }

    explicit operator bool() const noexcept {
        return fd_ != nullptr && *fd_ >= 0;
    }

    bool operator==(const FileDescriptor& other) const noexcept {
        return handle() == other.handle();
    }

  private:
    std::shared_ptr<int> fd_{nullptr};
};
