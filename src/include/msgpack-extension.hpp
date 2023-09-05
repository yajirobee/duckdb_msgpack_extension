#pragma once

#include "buffered_msgpack_reader.hpp"
#include "duckdb.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct MsgpackScanData : public TableFunctionData {
public:
  void Bind(ClientContext &context, TableFunctionBindInput &input);

  void SetCompression(const string &compression);

public:
  //! File-specific options
  BufferedMsgpackReaderOptions options;

  //! The files we're reading
  vector<string> files;
  //! Initial file reader
  unique_ptr<BufferedMsgpackReader> initial_reader;
  //! The readers
  vector<unique_ptr<BufferedMsgpackReader>> union_readers;

  //! Maximum messagepack oject size (defaults to 16MB minimum)
  idx_t maximum_object_size = 16777216;

  //! All column names (in order)
  vector<string> names;
};

struct MsgpackScanInfo : public TableFunctionInfo {};

struct MsgpackScanGlobalState {
public:
  MsgpackScanGlobalState(ClientContext &context,
                         const MsgpackScanData &bind_data);

public:
  //! Bound data
  const MsgpackScanData &bind_data;

  //! Column names that we're actually reading (after projection pushdown)
  vector<string> names;
  vector<column_t> column_indices;

  //! Buffer manager allocator
  Allocator &allocator;
  //! The current buffer capacity
  idx_t buffer_capacity;

  //! One Msgpack reader per file
  vector<optional_ptr<BufferedMsgpackReader>> msgpack_readers;
  //! Current file/batch index
  idx_t file_index;
  atomic<idx_t> batch_index;
};

struct MsgpackScanLocalState {
public:
  MsgpackScanLocalState(ClientContext &context, MsgpackScanGlobalState &gstate);

public:
  idx_t ReadNext(MsgpackScanGlobalState &gstate);

public:
  //! Current scan data
  idx_t scan_count;

  //! Batch index for order-preserving parallelism
  idx_t batch_index;

  //! For determining average tuple size
  idx_t total_read_size;
  idx_t total_tuple_count;
};

struct MsgpackGlobalTableFunctionState : public GlobalTableFunctionState {
public:
  MsgpackGlobalTableFunctionState(ClientContext &context,
                                  TableFunctionInitInput &input);
  static unique_ptr<GlobalTableFunctionState>
  Init(ClientContext &context, TableFunctionInitInput &input);

public:
  MsgpackScanGlobalState state;
};

struct MsgpackLocalTableFunctionState : public LocalTableFunctionState {
public:
  MsgpackLocalTableFunctionState(ClientContext &context,
                                 MsgpackScanGlobalState &gstate);
  static unique_ptr<LocalTableFunctionState>
  Init(ExecutionContext &context, TableFunctionInitInput &input,
       GlobalTableFunctionState *global_state);

public:
  MsgpackScanLocalState state;
};

class MsgpackExtension : public Extension {
public:
  void Load(DuckDB &db) override;
  std::string Name() override;
};

} // namespace duckdb
