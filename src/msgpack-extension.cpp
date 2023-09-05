#define DUCKDB_EXTENSION_MAIN
#include <msgpack.hpp>

#include "include/msgpack-extension.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "msgpack-extension.hpp"

namespace duckdb {
void MsgpackScanData::Bind(ClientContext &context,
                           TableFunctionBindInput &input) {
  for (auto &kv : input.named_parameters) {
    if (MultiFileReader::ParseOption(kv.first, kv.second,
                                     options.file_options)) {
      continue;
    }
    auto loption = StringUtil::Lower(kv.first);
    if (loption == "compression") {
      SetCompression(StringUtil::Lower(StringValue::Get(kv.second)));
    }
  }

  files = MultiFileReader::GetFileList(context, input.inputs[0], "Msgpack");

  union_readers.resize(files.empty() ? 0 : files.size() - 1);
  for (idx_t file_idx = 0; file_idx < files.size(); file_idx++) {
    if (file_idx == 0) {
      initial_reader =
          make_uniq<BufferedMsgpackReader>(context, options, files[0]);
    } else {
      union_readers[file_idx - 1] =
          make_uniq<BufferedMsgpackReader>(context, options, files[file_idx]);
    }
  }
}

void MsgpackScanData::SetCompression(const string &compression) {
  options.compression =
      EnumUtil::FromString<FileCompressionType>(StringUtil::Upper(compression));
}

MsgpackScanGlobalState::MsgpackScanGlobalState(
    ClientContext &context, const MsgpackScanData &bind_data_p)
    : bind_data(bind_data_p),
      allocator(BufferManager::GetBufferManager(context).GetBufferAllocator()),
      buffer_capacity(bind_data.maximum_object_size * 2), file_index(0),
      batch_index(0) {}

MsgpackScanLocalState::MsgpackScanLocalState(ClientContext &context,
                                             MsgpackScanGlobalState &gstate)
    : scan_count(0), batch_index(DConstants::INVALID_INDEX), total_read_size(0),
      total_tuple_count(0) {}

idx_t MsgpackScanLocalState::ReadNext(MsgpackScanGlobalState &gstate) {
  if (scan_count == 0) {
    return ++scan_count;
  } else {
    return 0;
  }
}

MsgpackGlobalTableFunctionState::MsgpackGlobalTableFunctionState(
    ClientContext &context, TableFunctionInitInput &input)
    : state(context, input.bind_data->Cast<MsgpackScanData>()) {}

unique_ptr<GlobalTableFunctionState>
MsgpackGlobalTableFunctionState::Init(ClientContext &context,
                                      TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<MsgpackScanData>();
  auto result = make_uniq<MsgpackGlobalTableFunctionState>(context, input);
  auto &gstate = result->state;

  // Perform projection pushdown
  for (idx_t col_idx = 0; col_idx < input.column_ids.size(); col_idx++) {
    const auto &col_id = input.column_ids[col_idx];

    gstate.column_indices.push_back(col_idx);
    gstate.names.push_back(bind_data.names[col_id]);
  }

  return std::move(result);
}

MsgpackLocalTableFunctionState::MsgpackLocalTableFunctionState(
    ClientContext &context, MsgpackScanGlobalState &gstate)
    : state(context, gstate) {}

unique_ptr<LocalTableFunctionState>
MsgpackLocalTableFunctionState::Init(ExecutionContext &context,
                                     TableFunctionInitInput &input,
                                     GlobalTableFunctionState *global_state) {
  auto &gstate = global_state->Cast<MsgpackGlobalTableFunctionState>();
  auto result =
      make_uniq<MsgpackLocalTableFunctionState>(context.client, gstate.state);

  return std::move(result);
}

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
  output.SetCardinality(count);

  if (!gstate.names.empty()) {
    vector<Vector *> result_vectors;
    result_vectors.reserve(gstate.column_indices.size());
    for (const auto &col_idx : gstate.column_indices) {
      result_vectors.emplace_back(&output.data[col_idx]);
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
