.PHONY: all clean debug release pull format

all: release

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJ_DIR := $(dir $(MKFILE_PATH))

#### VCPKG config
VCPKG_TOOLCHAIN_PATH?=
ifneq ("${VCPKG_TOOLCHAIN_PATH}", "")
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_MANIFEST_DIR='${PROJ_DIR}' -DVCPKG_BUILD=1 -DCMAKE_TOOLCHAIN_FILE='${VCPKG_TOOLCHAIN_PATH}'
endif
ifneq ("${VCPKG_TARGET_TRIPLET}", "")
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_TARGET_TRIPLET='${VCPKG_TARGET_TRIPLET}'
endif

#### Configuration for this extension
EXTENSION_NAME=DUCKDB_MSGPACK
EXTENSION_FLAGS=\
-DDUCKDB_EXTENSION_NAMES="duckdb_msgpack" \
-DDUCKDB_EXTENSION_${EXTENSION_NAME}_PATH="$(PROJ_DIR)" \
-DDUCKDB_EXTENSION_${EXTENSION_NAME}_LOAD_TESTS=1 \
-DDUCKDB_EXTENSION_${EXTENSION_NAME}_INCLUDE_PATH="$(PROJ_DIR)src/include" \
-DDUCKDB_EXTENSION_${EXTENSION_NAME}_TEST_PATH="$(PROJ_DIR)test/sql"

#### Add more of the DuckDB in-tree extensions here that you need (also feel free to remove them when not needed)
EXTRA_EXTENSIONS_FLAG=-DBUILD_EXTENSIONS="tpch;visualizer"

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=0 $(EXTENSION_FLAGS) ${EXTRA_EXTENSIONS_FLAG} $(TOOLCHAIN_FLAGS)
CLIENT_FLAGS:=

#### Main build
# For regular CLI build, we link the quack extension directly into the DuckDB executable
CLIENT_FLAGS=-DDUCKDB_EXTENSION_${EXTENSION_NAME}_SHOULD_LINK=1

debug:
	mkdir -p  build/debug && \
	cmake $(GENERATOR) $(BUILD_FLAGS) $(CLIENT_FLAGS) -DCMAKE_BUILD_TYPE=Debug -S ./duckdb/ -B build/debug && \
	cmake --build build/debug --config Debug

release:
	mkdir -p build/release && \
	cmake $(GENERATOR) $(BUILD_FLAGS)  $(CLIENT_FLAGS) -DCMAKE_BUILD_TYPE=Release -S ./duckdb/ -B build/release && \
	cmake --build build/release --config Release

#### Misc
format:
	find src/ -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt

update:
	git submodule update --remote --merge

pull:
	git submodule init
	git submodule update --recursive --remote

clean:
	rm -rf build
	rm -rf testext
	cd duckdb && make clean
	cd duckdb && make clean-python
