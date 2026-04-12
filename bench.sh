#!/bin/bash

set -e

PORT=9999
EPOLL_DIR="./build/bin/epoll"
URING_DIR="./build/bin/uring"
PINGPONG="./build/pingpong_client"
LOG_FILE="build/bench.log"

exec 2>&1

cleanup() {
    kill $SERVER_PID 2>/dev/null || true
    sleep 0.3
}
trap cleanup EXIT

wait_for_server() {
    local port=$1
    local max_attempts=30
    local attempt=0
    while ! nc -z 127.0.0.1 $port 2>/dev/null; do
        sleep 0.1
        attempt=$((attempt + 1))
        if [ $attempt -ge $max_attempts ]; then
            echo "FAIL: Server on port $port failed to start"
            exit 1
        fi
    done
}

print_header() {
    echo ""
    echo "========================================"
    echo "$1"
    echo "========================================"
}

echo "Building project..."
echo ""
cmake -S . -B build -G "Ninja" && cmake --build build/ > /dev/null 2>&1

echo "====================================="
echo "  EPOLL Backend Tests"
echo "====================================="

print_header "Test 1: echo_client.cc (epoll)"
$EPOLL_DIR/echo_server_epoll > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
wait_for_server $PORT
if $EPOLL_DIR/echo_client_epoll > "$LOG_FILE" 2>&1; then
    echo "[PASS] echo_client_epoll"
else
    echo "[FAIL] echo_client_epoll"
    exit 1
fi
kill $SERVER_PID 2>/dev/null; wait; sleep 0.5

print_header "Test 2: echo_client_buf.cc (epoll)"
$EPOLL_DIR/echo_server_epoll > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
wait_for_server $PORT
if $EPOLL_DIR/echo_client_buf_epoll > "$LOG_FILE" 2>&1; then
    echo "[PASS] echo_client_buf_epoll"
else
    echo "[FAIL] echo_client_buf_epoll"
    exit 1
fi
kill $SERVER_PID 2>/dev/null; wait; sleep 0.5

print_header "Test 3: pingpong_client (epoll)"
echo "Running: 1000 conn, 4 thr, 3s, 4096B"
$EPOLL_DIR/echo_server_epoll > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
wait_for_server $PORT
EPOLL_PINGPONG=$($PINGPONG -c 1000 -t 4 -d 3 -s 4096 2>&1 | grep -oP 'throughput = \K[0-9.]+')
echo "Result: $EPOLL_PINGPONG MB/s"
[ -z "$EPOLL_PINGPONG" ] && exit 1
kill $SERVER_PID 2>/dev/null; wait; sleep 0.5

print_header "Test 4: ab HTTP (epoll)"
$EPOLL_DIR/http_server_epoll > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
wait_for_server $PORT

echo "ab (no -k): 50k reqs, 50 conc"
EPOLL_HTTP=$(ab -n 10000 -c 50 http://127.0.0.1:$PORT/ 2>&1 | grep "Requests per second" | awk '{print $4}')
echo "Result: $EPOLL_HTTP req/s"

echo "ab (-k): 50k reqs, 50 conc"
EPOLL_HTTP_K=$(ab -n 10000 -c 50 -k http://127.0.0.1:$PORT/ 2>&1 | grep "Requests per second" | awk '{print $4}' | head -1)
echo "Result: $EPOLL_HTTP_K req/s"

kill $SERVER_PID 2>/dev/null; wait; sleep 0.5

echo ""
echo "====================================="
echo "  IO_URING Backend Tests"
echo "====================================="

print_header "Test 1: echo_client.cc (iouring)"
$URING_DIR/echo_server_uring > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
wait_for_server $PORT
if $URING_DIR/echo_client_uring > "$LOG_FILE" 2>&1; then
    echo "[PASS] echo_client_uring"
else
    echo "[FAIL] echo_client_uring"
    exit 1
fi
kill $SERVER_PID 2>/dev/null; wait; sleep 0.5

print_header "Test 2: echo_client_buf.cc (iouring)"
$URING_DIR/echo_server_uring > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
wait_for_server $PORT
if $URING_DIR/echo_client_buf_uring > "$LOG_FILE" 2>&1; then
    echo "[PASS] echo_client_buf_uring"
else
    echo "[FAIL] echo_client_buf_uring"
    exit 1
fi
kill $SERVER_PID 2>/dev/null; wait; sleep 0.5

print_header "Test 3: pingpong_client (iouring)"
echo "Running: 1000 conn, 4 thr, 3s, 4096B"
$URING_DIR/echo_server_uring > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
wait_for_server $PORT
URING_PINGPONG=$($PINGPONG -c 1000 -t 4 -d 3 -s 4096 2>&1 | grep -oP 'throughput = \K[0-9.]+')
echo "Result: $URING_PINGPONG MB/s"
[ -z "$URING_PINGPONG" ] && exit 1
kill $SERVER_PID 2>/dev/null; wait; sleep 0.5

print_header "Test 4: ab HTTP (iouring)"
$URING_DIR/http_server_uring > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
wait_for_server $PORT

echo "ab (no -k): 50k reqs, 50 conc"
URING_HTTP=$(ab -n 10000 -c 50 http://127.0.0.1:$PORT/ 2>&1 | grep "Requests per second" | awk '{print $4}')
echo "Result: $URING_HTTP req/s"

echo "ab (-k): 50k reqs, 50 conc"
URING_HTTP_K=$(ab -n 10000 -c 50 -k http://127.0.0.1:$PORT/ 2>&1 | grep "Requests per second" | awk '{print $4}' | head -1)
echo "Result: $URING_HTTP_K req/s"

kill $SERVER_PID 2>/dev/null; wait

print_header "Benchmark Results Summary"
echo ""
echo "=============================================="
echo "         Performance Comparison"
echo "=============================================="
echo ""

print_benchmark() {
    local test_name="$1"
    local epoll_full="$2"
    local uring_full="$3"
    local winner="${4:-Epoll}"
    # 严格固定列宽：
    # - 测试名：左对齐，总40字符
    # - Epoll/IoUring指标：右对齐，总15字符
    # - Winner：右对齐，总15字符
    printf "%-15s %15s %15s %15s\n" "$test_name" "$epoll_full" "$uring_full" "$winner"
}
# 3. 输出表头（严格按列宽）
print_benchmark "Test" "Epoll" "IoUring" "Winner"
# 4. 输出分隔线（严格按列宽生成短横线）
print_benchmark "---------------" "---------------" "---------------" "---------------"
# 5. 比较逻辑+输出各测试结果
WINNER="Epoll"
[ "$(echo "$URING_PINGPONG > $EPOLL_PINGPONG" | bc -l 2>/dev/null)" = "1" ] && WINNER="IoUring"
print_benchmark "pingpong" "$EPOLL_PINGPONG MB/s" "$URING_PINGPONG MB/s" "$WINNER"
WINNER="Epoll"
[ "$(echo "$URING_HTTP > $EPOLL_HTTP" | bc -l 2>/dev/null)" = "1" ] && WINNER="IoUring"
print_benchmark "http" "$EPOLL_HTTP req/s" "$URING_HTTP req/s" "$WINNER"
WINNER="Epoll"
[ "$(echo "$URING_HTTP_K > $EPOLL_HTTP_K" | bc -l 2>/dev/null)" = "1" ] && WINNER="IoUring"
print_benchmark "http -k" "$EPOLL_HTTP_K req/s" "$URING_HTTP_K req/s" "$WINNER"

echo ""
echo "All tests passed!"
