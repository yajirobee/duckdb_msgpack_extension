cmake_minimum_required(VERSION 3.10)

# extension name "msgpack" is unavailable as "char* msgpack_version(void)" signiture is
# already taken by msgpack-cxx library.
set(TARGET_NAME msgpack_maps)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

find_package(msgpack-cxx REQUIRED)

include_directories(src/include)

set(EXTENSION_SOURCES
    src/msgpack_maps_extension.cpp
    src/msgpack_scan.cpp
    src/buffered_msgpack_reader.cpp
    src/msgpack_transform.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} msgpack-cxx)
target_link_libraries(${LOADABLE_EXTENSION_NAME} msgpack-cxx)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
