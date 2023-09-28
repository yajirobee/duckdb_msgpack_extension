# DuckDB MessagePack extension

The MessagePack extension allows DuckDB to directly read data from files storing [MessagePack](https://msgpack.org/) map values.

## Disclaimer
This is just a toy project. Do not use for production systems.

## Dependency
- [DuckDB](https://github.com/duckdb/duckdb) version 0.8.1 or above
- [msgpack for C++](https://github.com/msgpack/msgpack-c/tree/cpp_master) version 6.0.0 or above

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
LOAD 'build/release/extension/duckdb_msgpack_extension/src/msgpack_ext.duckdb_extension';
```
