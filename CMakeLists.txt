cmake_minimum_required(VERSION 3.21)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(c++coro LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# set(CMAKE_BUILD_TYPE "Release")
set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O1 -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined -fsanitize-address-use-after-scope")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize=undefined")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address -fsanitize=undefined")
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
add_library(c++co SHARED src/log.cc src/excutor.cc src/async.cc src/thrdpool.cc src/co_excutor.cc src/excutor_epoll.cc src/excutor_iouring.cc)
target_link_libraries(c++co PUBLIC uring)


add_executable(task_test test/task_test.cc)
target_link_libraries(task_test PRIVATE c++co)

add_executable(echo_server test/echo_server.cc)
target_link_libraries(echo_server PRIVATE c++co)

add_executable(echo_client test/echo_client.cc)
target_link_libraries(echo_client PRIVATE c++co)

add_executable(echo_client_mt test/echo_client_mt.cc)
target_link_libraries(echo_client_mt PRIVATE c++co)

add_executable(echo_client_buf test/echo_client_buf.cc)
target_link_libraries(echo_client_buf PRIVATE c++co)

add_executable(http_server test/http_server.cc)
target_link_libraries(http_server PRIVATE c++co)

add_executable(thrd_test test/thrd_test.cc)
target_link_libraries(thrd_test PRIVATE c++co)

add_executable(pingpong_client test/pingpong_client.cc)
target_link_libraries(pingpong_client PRIVATE pthread)
