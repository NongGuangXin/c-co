.PHONY: all prebuild build run clean release

all: build

prebuild:
	@mkdir -p build

build: prebuild
	@cmake -S . -B build -G "Ninja" $(CMAKE_EXTRA_ARGS)
	@cmake --build build/

release: prebuild
	@cmake -S . -B build -G "Ninja" -DCMAKE_BUILD_TYPE=Release $(CMAKE_EXTRA_ARGS)
	@cmake --build build/

with_spdlog: prebuild
	@cmake -S . -B build -G "Ninja" -DCMAKE_BUILD_TYPE=Release -DUSE_SPDLOG=ON $(CMAKE_EXTRA_ARGS)
	@cmake --build build/

run: build
	@ ./build/main

clean:
	@if [ -e build/compile_commands.json ]; then cp build/compile_commands.json /tmp/ ; fi
	@rm -rf build
	@if [ -e /tmp/compile_commands.json ]; then mkdir -p build && cp /tmp/compile_commands.json build/ ; fi
