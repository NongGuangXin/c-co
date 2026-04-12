cmake_minimum_required(VERSION 3.21)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(c++coro LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# set(CMAKE_BUILD_TYPE "Release")
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()
option(USE_SPDLOG "Enable spdlog logging" OFF)
if(USE_SPDLOG)
    add_compile_definitions(USE_SPDLOG)
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -march=native -DNDEBUG")
elseif(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O1 -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined -fsanitize-address-use-after-scope")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize=undefined")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address -fsanitize=undefined")
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Common sources (excluding backend-specific files)
set(COMMON_SRCS src/log.cc src/thrdpool.cc src/async.cc src/co_excutor.cc)

# Test executables to build for each backend
set(TEST_TARGETS
    task_test:test/task_test.cc
    echo_server:test/echo_server.cc
    echo_client:test/echo_client.cc
    echo_client_mt:test/echo_client_mt.cc
    echo_client_buf:test/echo_client_buf.cc
    http_server:test/http_server.cc
    thrd_test:test/thrd_test.cc
)

# --- epoll backend ---
add_library(c++co_epoll SHARED ${COMMON_SRCS} src/excutor_epoll.cc)
target_link_libraries(c++co_epoll PUBLIC pthread)
if(USE_SPDLOG)
    target_link_libraries(c++co_epoll PUBLIC spdlog fmt)
endif()

foreach(entry ${TEST_TARGETS})
    string(REPLACE ":" ";" parts ${entry})
    list(GET parts 0 name)
    list(GET parts 1 src)
    set(target_name "${name}_epoll")
    add_executable(${target_name} ${src})
    target_link_libraries(${target_name} PRIVATE c++co_epoll)
    set_target_properties(${target_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/epoll)
endforeach()

# --- uring backend ---
add_library(c++co_uring SHARED ${COMMON_SRCS} src/excutor_iouring.cc)
target_compile_definitions(c++co_uring PUBLIC USE_URING)
target_link_libraries(c++co_uring PUBLIC uring)
if(USE_SPDLOG)
    target_link_libraries(c++co_uring PUBLIC spdlog fmt)
endif()

foreach(entry ${TEST_TARGETS})
    string(REPLACE ":" ";" parts ${entry})
    list(GET parts 0 name)
    list(GET parts 1 src)
    set(target_name "${name}_uring")
    add_executable(${target_name} ${src})
    target_link_libraries(${target_name} PRIVATE c++co_uring)
    set_target_properties(${target_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/uring)
endforeach()

# pingpong_client has no backend dependency
add_executable(pingpong_client test/pingpong_client.cc)
target_link_libraries(pingpong_client PRIVATE pthread)
