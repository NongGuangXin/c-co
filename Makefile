.PHONY: all prebuild build run clean

all: build

prebuild:
	@mkdir -p build

build: prebuild
	@cmake -S . -B build -G "Ninja"
	@cmake --build build/

run: build
	@ ./build/main

clean:
	@if [ -e build/compile_commands.json ]; then cp build/compile_commands.json /tmp/ ; fi
	@rm -rf build
	@if [ -e /tmp/compile_commands.json ]; then mkdir -p build && cp /tmp/compile_commands.json build/ ; fi

# for i in $(seq 1 50); do timeout 2 ./test_server.py 2>&1 > /dev/null; done && echo "50次运行完成，无卡死"
# for i in $(seq 1 50); do timeout 2 ./build/echo_client 2>&1 > /dev/null; done && echo "50次运行完成，无卡死"
