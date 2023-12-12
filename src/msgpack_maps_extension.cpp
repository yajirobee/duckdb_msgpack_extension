#define DUCKDB_EXTENSION_MAIN

#include <msgpack.hpp>

#include "msgpack_maps_extension.hpp"
#include "msgpack_scan.hpp"
#include "msgpack_transform.hpp"

#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {
struct MsgpackGlobalTableFunctionState : public GlobalTableFunctionState {
public:
  MsgpackGlobalTableFunctionState(ClientContext &context,
                                  TableFunctionInitInput &input)
      : state(context, input.bind_data->Cast<MsgpackScanData>()) {}

  MsgpackScanGlobalState state;
};

struct MsgpackLocalTableFunctionState : public LocalTableFunctionState {
public:
  MsgpackLocalTableFunctionState(ClientContext &context,
                                 MsgpackScanGlobalState &gstate)
      : state(context, gstate) {}

  MsgpackScanLocalState state;
};

static unique_ptr<FunctionData>
ReadMsgpackBind(ClientContext &context, TableFunctionBindInput &input,
                vector<LogicalType> &return_types, vector<string> &names) {
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

static unique_ptr<GlobalTableFunctionState>
ReadMsgpackInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<MsgpackScanData>();
  auto result = make_uniq<MsgpackGlobalTableFunctionState>(context, input);
  auto &gstate = result->state;

  // Perform projection pushdown
  for (idx_t col_idx = 0; col_idx < input.column_ids.size(); col_idx++) {
    const auto &col_id = input.column_ids[col_idx];

    gstate.column_indices.push_back(col_idx);
    gstate.names.push_back(bind_data.names[col_id]);
  }

  // Place readers where they belong
  if (bind_data.initial_reader) {
    bind_data.initial_reader->Reset();
    gstate.msgpack_readers.emplace_back(bind_data.initial_reader.get());
  }
  for (const auto &reader : bind_data.union_readers) {
    reader->Reset();
    gstate.msgpack_readers.emplace_back(reader.get());
  }

  return std::move(result);
}

static unique_ptr<LocalTableFunctionState>
ReadMsgpackInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                     GlobalTableFunctionState *global_state) {
  auto &gstate = global_state->Cast<MsgpackGlobalTableFunctionState>();
  auto result =
      make_uniq<MsgpackLocalTableFunctionState>(context.client, gstate.state);

  return std::move(result);
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
  output.SetCardinality(row_count);

  if (!gstate.names.empty() && row_count > 0) {
    const auto column_count = gstate.column_indices.size();
    vector<Vector *> result_vectors;
    result_vectors.reserve(column_count);
    for (const auto &col_idx : gstate.column_indices) {
      result_vectors.emplace_back(&output.data[col_idx]);
    }

    // convert rows to columns
    unordered_map<std::string, idx_t> key_map;
    vector<msgpack::object **> values_by_column;
    values_by_column.reserve(column_count);
    for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
      key_map.insert({gstate.names[col_idx], col_idx});
      values_by_column.push_back(
          AllocateArray<msgpack::object *>(gstate.allocator, row_count));
    }

    idx_t found_key_count;
    auto found_keys = AllocateArray<bool>(gstate.allocator, column_count);

    for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
      const auto obj = lstate.values[row_idx].get();
      if (obj.type != msgpack::type::MAP) {
        throw msgpack::type_error();
      }

      found_key_count = 0;
      memset(found_keys, false, column_count);

      msgpack::object_kv *p(obj.via.map.ptr);
      msgpack::object_kv *const pend(obj.via.map.ptr + obj.via.map.size);
      for (; p != pend; ++p) {
        std::string key;
        p->key.convert(key);
        auto it = key_map.find(key);
        if (it != key_map.end()) {
          const auto &col_idx = it->second;
          // TODO: handle duplicate key
          values_by_column[col_idx][row_idx] = &p->val;
          found_keys[col_idx] = true;
          found_key_count++;
        }
        // TODO: handle unknown keys
      }

      if (found_key_count != column_count) {
        for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
          if (!found_keys[col_idx]) {
            throw InvalidInputException("Object doesn't have key \"" +
                                        gstate.names[col_idx] + "\"");
          }
        }
      }
    }

    // transform msgpack values
    for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
      auto result = *result_vectors[col_idx];
      Transform(values_by_column[col_idx], result, row_count);
    }
  }
}

void MsgpackMapsExtension::Load(DuckDB &db) {
  auto &db_instance = *db.instance;

  TableFunction table_function("read_msgpack", {LogicalType::VARCHAR},
                               ReadMsgpackFunction, ReadMsgpackBind,
                               ReadMsgpackInitGlobal, ReadMsgpackInitLocal);

  table_function.named_parameters["compression"] = LogicalType::VARCHAR;

  table_function.named_parameters["columns"] = LogicalType::ANY;

  auto info = make_shared<MsgpackScanInfo>();
  table_function.function_info = std::move(info);
  auto function = MultiFileReader::CreateFunctionSet(table_function);
  ExtensionUtil::RegisterFunction(db_instance, function);
}

std::string MsgpackMapsExtension::Name() { return "msgpack_maps"; }
} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void msgpack_maps_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::MsgpackMapsExtension>();
}

DUCKDB_EXTENSION_API const char *msgpack_maps_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
