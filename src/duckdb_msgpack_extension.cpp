#define DUCKDB_EXTENSION_MAIN

#include <msgpack.hpp>

#include "duckdb_msgpack_extension.hpp"

#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {
unique_ptr<FunctionData> ReadMsgpackBind(ClientContext &context,
                                         TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types,
                                         vector<string> &names) {
  auto bind_data = make_uniq<MsgpackScanData>();
  bind_data->Bind(context, input);

  for (auto &kv : input.named_parameters) {
    auto loption = StringUtil::Lower(kv.first);
    if (loption == "columns") {
      auto &child_type = kv.second.type();
      if (child_type.id() != LogicalTypeId::STRUCT) {
        throw BinderException(
            "read_msgpack \"columns\" parameter requires a struct as input.");
      }
      auto &struct_children = StructValue::GetChildren(kv.second);
      D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
      for (idx_t i = 0; i < struct_children.size(); i++) {
        auto &name = StructType::GetChildName(child_type, i);
        auto &val = struct_children[i];
        names.push_back(name);
        if (val.type().id() != LogicalTypeId::VARCHAR) {
          throw BinderException("read_msgpack \"columns\" parameter type "
                                "specification must be VARCHAR.");
        }
        return_types.emplace_back(
            TransformStringToLogicalType(StringValue::Get(val), context));
      }
      D_ASSERT(names.size() == return_types.size());
      if (names.empty()) {
        throw BinderException(
            "read_msgpack \"columns\" parameter needs at least one column.");
      }
      bind_data->names = names;
    }
  }
  return std::move(bind_data);
}

template <class T> static T *AllocateArray(Allocator &allocator, idx_t count) {
  return reinterpret_cast<T *>(allocator.AllocateData(sizeof(T) * count));
}

static void ReadMsgpackFunction(ClientContext &context,
                                TableFunctionInput &data_p, DataChunk &output) {
  auto &gstate =
      data_p.global_state->Cast<MsgpackGlobalTableFunctionState>().state;
  auto &lstate =
      data_p.local_state->Cast<MsgpackLocalTableFunctionState>().state;

  const auto row_count = lstate.ReadNext(gstate);
  std::unique_ptr<msgpack::object_handle> *values = lstate.values;
  output.SetCardinality(row_count);

  if (!gstate.names.empty()) {
    const auto column_count = gstate.column_indices.size();
    vector<Vector *> result_vectors;
    result_vectors.reserve(column_count);
    for (const auto &col_idx : gstate.column_indices) {
      result_vectors.emplace_back(&output.data[col_idx]);
    }

    // convert rows to columns
    vector<msgpack::object **> values_by_column;
    values_by_column.reserve(column_count);
    for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
      values_by_column.push_back(
          AllocateArray<msgpack::object *>(gstate.allocator, row_count));
    }

    for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
      std::map<std::string, msgpack::object> row =
          values[row_idx]->get().convert();
      for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
        *values_by_column[col_idx][row_idx] = row[gstate.names[col_idx]];
      }
    }

    // transform msgpack values
    for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
      auto result = *result_vectors[col_idx];
      std::cout << "col_idx: " << col_idx
                << ", column name: " << gstate.names[col_idx] << std::endl;
      Transform(values_by_column[col_idx], result, row_count);
    }
  }
}

void DuckdbMsgpackExtension::Load(DuckDB &db) {
  auto &db_instance = *db.instance;

  TableFunction table_function({LogicalType::VARCHAR}, ReadMsgpackFunction,
                               ReadMsgpackBind,
                               MsgpackGlobalTableFunctionState::Init,
                               MsgpackLocalTableFunctionState::Init);
  table_function.name = "read_msgpack";

  table_function.named_parameters["compression"] = LogicalType::VARCHAR;

  table_function.named_parameters["columns"] = LogicalType::ANY;

  auto info = make_shared<MsgpackScanInfo>();
  table_function.function_info = std::move(info);
  auto function = MultiFileReader::CreateFunctionSet(table_function);
  ExtensionUtil::RegisterFunction(db_instance, function);
}

std::string DuckdbMsgpackExtension::Name() { return "duckdb_msgpack"; }
} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void msgpack_ext_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::DuckdbMsgpackExtension>();
}

DUCKDB_EXTENSION_API const char *msgpack_ext_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
