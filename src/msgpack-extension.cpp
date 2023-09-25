#define DUCKDB_EXTENSION_MAIN

#include <msgpack.hpp>

#include "msgpack-extension.hpp"

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

static void ReadMsgpackFunction(ClientContext &context,
                                TableFunctionInput &data_p, DataChunk &output) {
  auto &gstate =
      data_p.global_state->Cast<MsgpackGlobalTableFunctionState>().state;
  auto &lstate =
      data_p.local_state->Cast<MsgpackLocalTableFunctionState>().state;

  const auto count = lstate.ReadNext(gstate);
  msgpack::object_handle **values = lstate.values;
  output.SetCardinality(count);

  if (!gstate.names.empty()) {
    vector<Vector *> result_vectors;
    result_vectors.reserve(gstate.column_indices.size());
    for (const auto &col_idx : gstate.column_indices) {
      result_vectors.emplace_back(&output.data[col_idx]);
    }

    for (idx_t i = 0; i < count; i++) {
      std::cout << values[i]->get().type << std::endl;
    }
    for (idx_t col_idx = 0; col_idx < gstate.names.size(); col_idx++) {
      auto data = FlatVector::GetData<int32_t>(*result_vectors[col_idx]);
      data[0] = 1;
    }
  }
}

void MsgpackExtension::Load(DuckDB &db) {
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

std::string MsgpackExtension::Name() { return "msgpack"; }
} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void msgpack_ext_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::MsgpackExtension>();
}

DUCKDB_EXTENSION_API const char *msgpack_ext_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}
