find_package(msgpack-cxx REQUIRED)
include_directories(include)
set(MSGPACK_EXTENSION_FILES msgpack-extension.cpp)

set(PARAMETERS "-warnings")
build_loadable_extension(msgpack_ext ${PARAMETERS} ${MSGPACK_EXTENSION_FILES})

# target name comes from https://github.com/duckdb/duckdb/blob/6536a772329002b05decbfc0a9d3f606e0ec7f55/CMakeLists.txt#L796
target_link_libraries(msgpack_ext_loadable_extension PRIVATE msgpack-cxx)
