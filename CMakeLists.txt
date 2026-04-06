cmake_minimum_required(VERSION 3.21)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(c++coro LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 通用 C++ 标志
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O1 -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined -fsanitize-address-use-after-scope")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize=undefined")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address -fsanitize=undefined")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
add_library(c++co SHARED src/log.cc src/excutor.cc src/async.cc)

add_executable(echo_server test/echo_server.cc)
target_link_libraries(echo_server PRIVATE c++co)

add_executable(echo_client test/echo_client.cc)
target_link_libraries(echo_client PRIVATE c++co)

add_executable(echo_client_mt test/echo_client_mt.cc)
target_link_libraries(echo_client_mt PRIVATE c++co)


add_executable(task_test test/task_test.cc)
target_link_libraries(task_test PRIVATE c++co)
