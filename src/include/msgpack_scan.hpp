#pragma once

#include <msgpack.hpp>

#include "buffered_msgpack_reader.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {
struct MsgpackScanData : public TableFunctionData {
public:
  //! File-specific options
  BufferedMsgpackReaderOptions options;

  //! The files we're reading
  unique_ptr<MultiFileList> files;
  unique_ptr<MultiFileReader> multi_file_reader;

  //! Initial file reader
  unique_ptr<BufferedMsgpackReader> initial_reader;
  //! The readers
  vector<unique_ptr<BufferedMsgpackReader>> union_readers;

  //! Maximum messagepack oject size (defaults to 16MB minimum)
  idx_t maximum_object_size = 16777216;

  //! All column names (in order)
  vector<string> names;

public:
  void Bind(ClientContext &context, TableFunctionBindInput &input) {
    multi_file_reader = MultiFileReader::Create(input.table_function);

    for (auto &kv : input.named_parameters) {
      if (multi_file_reader->ParseOption(kv.first, kv.second,
                                         options.file_options, context)) {
        continue;
      }
      auto loption = StringUtil::Lower(kv.first);
      if (loption == "compression") {
        SetCompression(StringUtil::Lower(StringValue::Get(kv.second)));
      }
    }

    files = multi_file_reader->CreateFileList(context, input.inputs[0]);

    union_readers.resize(files->IsEmpty() ? 0 : files->GetTotalFileCount() - 1);
    for (idx_t file_idx = 0; file_idx < files->GetTotalFileCount();
         file_idx++) {
      if (file_idx == 0) {
        initial_reader = make_uniq<BufferedMsgpackReader>(
            context, options, files->GetFirstFile());
      } else {
        union_readers[file_idx - 1] = make_uniq<BufferedMsgpackReader>(
            context, options, files->GetPaths()[file_idx]);
      }
    }
  }

  void SetCompression(const string &compression) {
    options.compression = EnumUtil::FromString<FileCompressionType>(
        StringUtil::Upper(compression));
  }
};

struct MsgpackScanInfo : public TableFunctionInfo {};

struct MsgpackScanGlobalState {
public:
  MsgpackScanGlobalState(ClientContext &context,
                         const MsgpackScanData &bind_data_p)
      : bind_data(bind_data_p),
        allocator(
            BufferManager::GetBufferManager(context).GetBufferAllocator()),
        buffer_capacity(bind_data.maximum_object_size * 2), file_index(0),
        batch_index(0),
        system_threads(TaskScheduler::GetScheduler(context).NumberOfThreads()) {
  }

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

  //! Thread-local allocator
  ArenaAllocator allocator;

  //! Current scan data
  idx_t scan_count;
  msgpack::object_handle values[STANDARD_VECTOR_SIZE];

  //! Batch index for order-preserving parallelism
  idx_t batch_index;

  //! For determining average tuple size
  idx_t total_read_size;
  idx_t total_tuple_count;

public:
  // return read tuple count
  idx_t ReadNext(MsgpackScanGlobalState &gstate);

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
  msgpack::object_handle ParseMsgpack();

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
  size_t buffer_size;
  size_t buffer_offset;
  idx_t prev_buffer_remainder;
  idx_t objects_in_buffer;

  //! Buffer to reconstruct split values
  AllocatedData reconstruct_buffer;
};
} // namespace duckdb
