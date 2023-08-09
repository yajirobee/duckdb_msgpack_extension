#define DUCKDB_BUILD_LOADABLE_EXTENSION

#include "msgpack-extension.hpp"
#include "duckdb/common/multi_file_reader.hpp"

namespace duckdb {
unique_ptr<FunctionData> ReadMsgpackBind(ClientContext &context,
                                         TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types,
                                         vector<string> &names) {}

static void ReadMsgpackFunction(ClientContext &context,
                                TableFunctionInput &data_p, DataChunk &output) {
}

void MsgpackExtension::Load(DuckDB &db) {
  auto &db_instance = *db.instance;

  TableFunction table_function({LogicalType::VARCHAR}, ReadMsgpackFunction,
                               ReadMsgpackBind,
                               MsgpackGlobalTableFunctionState::Init,
                               MsgpackLocalTableFunctionState::Init);
  table_funciton.name = "read_msgpack";
  MultiFileReader::CreateFunctionSet(table_function);
}

std::string MsgpackExtension::Name() { return "msgpack"; }
} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void msgpack_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::MsgpackExtension>();
}

DUCKDB_EXTENSION_API const char *msgpack_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}
