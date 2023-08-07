.PHONY: all clean build
all: build

clean:
	rm -rf build

pull:
	git submodule init
	git submodule update --recursive --remote

build:
	mkdir -p build && \
	cd build && \
	cmake ../ && \
	cmake --build .
