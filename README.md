# DuckDB MessagePack extension

The MessagePack extension allows DuckDB to directly read data from files storing [MessagePack](https://msgpack.org/) map values.

Please note this is just a toy project.

## Dependency
MessagePack extension requires [msgpack for C++](https://github.com/msgpack/msgpack-c/tree/cpp_master).

## Build
```sh
export CMAKE_BUILD_PARALLEL_LEVEL=6 # parallelize build
make (release/debug)
```

## Run
Run DuckDB CLI:
```sh
 ./build/release/duckdb -unsigned  # allow unsigned extensions
```

Then, load the extension:
```sql
LOAD 'build/release/extension/duckdb_msgpack_extension/msgpack.duckdb_extension';
```
