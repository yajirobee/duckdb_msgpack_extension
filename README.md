# DuckDB MessagePack extension

The MessagePack extension allows DuckDB to directly read data from files storing [MessagePack](https://msgpack.org/) map values.

## Disclaimer
This is just a toy project. Do not use for production systems.

## Dependency
- [DuckDB](https://github.com/duckdb/duckdb) version 0.9.0
- [msgpack for C++](https://github.com/msgpack/msgpack-c/tree/cpp_master) version 6.0.0 or above

## Build
```sh
export CMAKE_BUILD_PARALLEL_LEVEL=6 # parallelize build
# if you use vcpkg
export VCPKG_TOOLCHAIN_PATH="/path/to/your/vcpkg/installation"
make (release/debug)
```

## Run
`duckdb_msgpack_extension` is already linked into the built `duckdb` binary.
You can use the extension without load.

```sh
./build/release/duckdb
```

### Use loadable extension
Run DuckDB CLI:
```sh
duckdb -unsigned  # allow unsigned extensions
```

Then, load the extension:
```sql
LOAD 'build/release/extension/duckdb_msgpack_extension/duckdb_msgpack_ext.duckdb_extension';
```
