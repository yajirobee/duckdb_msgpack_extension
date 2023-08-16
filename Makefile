.PHONY: all clean debug release
all: release

MSGPACK_EXT_PATH=${PWD}

clean:
	rm -rf build

pull:
	git submodule init
	git submodule update --recursive --remote

debug:
	mkdir -p build/debug && \
	cd build/debug && \
	cmake -DCMAKE_BUILD_TYPE=Debug -DEXTENSION_STATIC_BUILD=0 ../../duckdb/CMakeLists.txt -DEXTERNAL_EXTENSION_DIRECTORIES=${MSGPACK_EXT_PATH} -B. -S ../../duckdb && \
	cmake --build .

release:
	mkdir -p build/release && \
	cd build/release && \
	cmake -DCMAKE_BUILD_TYPE=Release -DEXTENSION_STATIC_BUILD=0 ../../duckdb/CMakeLists.txt -DEXTERNAL_EXTENSION_DIRECTORIES=${MSGPACK_EXT_PATH} -B. -S ../../duckdb && \
	cmake --build .
