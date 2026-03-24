#include "fd.h"
#include <cmath>
#include <cstdio>
#include <unistd.h>

FileDescriptor::FileDescriptor(int fd):
    fd_(new int(fd), [](int* fd) {
        if(fd != nullptr && *fd > 0) { ::close(*fd); }
        if(fd != nullptr) { delete fd; }
    }) { }

int FileDescriptor::handle() const noexcept {
    if(fd_ != nullptr) { return (*fd_); }
    return -1;
}

FileDescriptor::operator bool() const noexcept {
    if(fd_ != nullptr) { return (*fd_) > 0; }
    return false;
}

bool FileDescriptor::operator==(const FileDescriptor& other) const noexcept {
    if(fd_ && other.fd_) { return *fd_ == *other.fd_; }
    return false;
}
