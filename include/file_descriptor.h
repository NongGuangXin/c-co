#pragma once

#include <memory>
#include <unistd.h>

// RAII file descriptor with shared ownership
// Optimized: custom control block avoids separate heap allocation for the int
class FileDescriptor {
  public:
    FileDescriptor() = default;

    explicit FileDescriptor(int fd) {
        // Wrap in a struct so make_shared allocates int + control block
        // together
        auto p  = std::make_shared<FdHolder>(fd);
        int* fp = &p->fd;
        fd_     = std::shared_ptr<int>(std::move(p), fp);
    }

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
    struct FdHolder {
        int fd;
        explicit FdHolder(int f): fd(f) { }
        ~FdHolder() {
            if(fd >= 0) { ::close(fd); }
        }
    };

    std::shared_ptr<int> fd_{nullptr};
};
