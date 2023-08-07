#define DUCKDB_BUILD_LOADABLE_EXTENSION

#include "msgpack-extension.hpp"

namespace duckdb {
  void MsgpackExtension::Load(DuckDB &db) {

  }

  std::string MsgpackExtension::Name() {
    return "msgpack";
  }
}

extern "C" {
DUCKDB_EXTENSION_API void msgpack_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::MsgpackExtension>();
}

DUCKDB_EXTENSION_API const char *msgpack_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}
