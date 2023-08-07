cmake_minimum_required(VERSION 3.10)

project(MessagePackExtension VERSION 1.0)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include_directories(include)
set(MSGPACK_EXTENSION_FILES msgpack-extension.cpp)

set(PARAMETERS "-warnings")
build_loadable_extension(msgpack ${PARAMETERS} ${MSGPACK_EXTENSION_FILES})
