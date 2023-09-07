#pragma once

#include <msgpack.hpp>

#include "buffered_msgpack_reader.hpp"
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

  mutex lock;
  //! One Msgpack reader per file
  vector<optional_ptr<BufferedMsgpackReader>> msgpack_readers;
  //! Current file/batch index
  idx_t file_index;
  atomic<idx_t> batch_index;

  //! Current number of threads active
  idx_t system_threads;
};

struct MsgpackScanLocalState {
public:
  MsgpackScanLocalState(ClientContext &context, MsgpackScanGlobalState &gstate);

public:
  // return read tuple count
  idx_t ReadNext(MsgpackScanGlobalState &gstate);

public:
  //! Current scan data
  idx_t scan_count;
  msgpack::object_handle *values[STANDARD_VECTOR_SIZE];

  //! Batch index for order-preserving parallelism
  idx_t batch_index;

  //! For determining average tuple size
  idx_t total_read_size;
  idx_t total_tuple_count;

private:
  // return true if read something
  bool ReadNextBuffer(MsgpackScanGlobalState &gstate);
  void ReadNextBufferInternal(MsgpackScanGlobalState &gstate,
                              idx_t &buffer_index);
  void ReadNextBufferSeek(MsgpackScanGlobalState &gstate, idx_t &buffer_index);
  void ReadNextBufferNoSeek(MsgpackScanGlobalState &gstate,
                            idx_t &buffer_index);
  void SkipOverArrayStart();

  void ParseNextChunk();

  void ParseMsgpack(char *const msgpack_start, const idx_t msgpack_size,
                    const idx_t remaining);
  void ThrowInvalidAtEndError();

  bool IsParallel(MsgpackScanGlobalState &gstate) const;

private:
  //! Bind data
  const MsgpackScanData &bind_data;

  //! Current reader and buffer handle
  optional_ptr<BufferedMsgpackReader> current_reader;
  optional_ptr<MsgpackBufferHandle> current_buffer_handle;

  //! Whether this is the last batch of the file
  bool is_last;

  //! Current buffer read info
  char *buffer_ptr;
  idx_t buffer_size;
  idx_t buffer_offset;
  idx_t prev_buffer_remainder;
  idx_t objects_in_buffer;

  //! Buffer to reconstruct split values
  AllocatedData reconstruct_buffer;
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
} // namespace duckdb
