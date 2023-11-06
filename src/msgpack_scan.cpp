#include "msgpack_scan.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "include/msgpack_scan.hpp"

namespace duckdb {
void MsgpackScanData::Bind(ClientContext &context,
                           TableFunctionBindInput &input) {
  for (auto &kv : input.named_parameters) {
    if (MultiFileReader::ParseOption(kv.first, kv.second, options.file_options,
                                     context)) {
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
      batch_index(0),
      system_threads(TaskScheduler::GetScheduler(context).NumberOfThreads()) {}

MsgpackScanLocalState::MsgpackScanLocalState(ClientContext &context,
                                             MsgpackScanGlobalState &gstate)
    : scan_count(0), batch_index(DConstants::INVALID_INDEX), total_read_size(0),
      total_tuple_count(0), bind_data(gstate.bind_data),
      allocator(BufferAllocator::Get(context)), current_reader(nullptr),
      current_buffer_handle(nullptr), is_last(false), buffer_size(0),
      buffer_offset(0), prev_buffer_remainder(0) {

  // Buffer to reconstruct Msgpack values when they cross a buffer boundary
  reconstruct_buffer = gstate.allocator.Allocate(gstate.buffer_capacity);
}

idx_t MsgpackScanLocalState::ReadNext(MsgpackScanGlobalState &gstate) {
  allocator.Reset();
  scan_count = 0;
  if (buffer_offset == buffer_size) {
    if (!ReadNextBuffer(gstate)) {
      return scan_count;
    }
    D_ASSERT(buffer_size != 0);
  }
  ParseNextChunk();

  return scan_count;
}

bool MsgpackScanLocalState::ReadNextBuffer(MsgpackScanGlobalState &gstate) {
  AllocatedData buffer;
  if (current_reader) {
    // Keep track of this for accurate errors
    current_reader->SetBufferLineOrObjectCount(
        current_buffer_handle->buffer_index, objects_in_buffer);

    // Try to re-use existing buffer
    if (current_buffer_handle && --current_buffer_handle->readers == 0) {
      buffer =
          current_reader->RemoveBuffer(current_buffer_handle->buffer_index);
    } else {
      buffer = gstate.allocator.Allocate(gstate.buffer_capacity);
    }

    if (!is_last) {
      memcpy(buffer.get(), reconstruct_buffer.get(),
             prev_buffer_remainder); // Copy last bit of previous buffer
    } else {
      current_reader->CloseMsgpackFile(); // Close files that are done
      current_reader = nullptr;
    }
  } else {
    buffer = gstate.allocator.Allocate(gstate.buffer_capacity);
  }
  buffer_ptr = char_ptr_cast(buffer.get());

  idx_t buffer_index;
  while (true) {
    if (current_reader) {
      ReadNextBufferInternal(gstate, buffer_index);
      if (buffer_size == 0) {
        if (is_last) {
          current_reader->CloseMsgpackFile();
        }
        if (IsParallel(gstate)) {
          // If this threads' current reader is still the one at
          // gstate.file_index, this thread can end the parallel scan
          lock_guard<mutex> guard(gstate.lock);
          if (gstate.file_index < gstate.msgpack_readers.size() &&
              current_reader ==
                  gstate.msgpack_readers[gstate.file_index].get()) {
            gstate.file_index++; // End parallel scan
          }
        }
        current_reader = nullptr;
      } else {
        break; // We read something!
      }
    }

    // This thread needs a new reader
    {
      lock_guard<mutex> guard(gstate.lock);
      if (gstate.file_index == gstate.msgpack_readers.size()) {
        return false; // No more files left
      }

      // Try the next reader
      current_reader = gstate.msgpack_readers[gstate.file_index].get();
      if (current_reader->IsOpen()) {
        // Can only be open from auto detection, so these should be known
        if (!IsParallel(gstate)) {
          batch_index = gstate.batch_index++;
          gstate.file_index++;
        }
        continue; // Re-enter the loop to start scanning the assigned file
      }

      current_reader->OpenMsgpackFile();
      batch_index = gstate.batch_index++;
      // Increment the file index within the lock, then read outside of the lock
      gstate.file_index++;
    }

    // High amount of files, just do 1 thread per file
    ReadNextBufferInternal(gstate, buffer_index);
    if (buffer_size == 0) {
      continue;
    }

    break;
  }
  D_ASSERT(buffer_size != 0); // We should have read something if we got here

  idx_t readers = is_last ? 1 : 2;

  // Create an entry and insert it into the map
  auto msgpack_buffer_handle = make_uniq<MsgpackBufferHandle>(
      buffer_index, readers, std::move(buffer), buffer_size);
  current_buffer_handle = msgpack_buffer_handle.get();
  current_reader->InsertBuffer(buffer_index, std::move(msgpack_buffer_handle));

  prev_buffer_remainder = 0;
  objects_in_buffer = 0;

  return true;
}

void MsgpackScanLocalState::ReadNextBufferInternal(
    MsgpackScanGlobalState &gstate, idx_t &buffer_index) {
  if (current_reader->GetFileHandle().CanSeek()) {
    ReadNextBufferSeek(gstate, buffer_index);
  } else {
    ReadNextBufferNoSeek(gstate, buffer_index);
  }

  buffer_offset = 0;
}

void MsgpackScanLocalState::ReadNextBufferSeek(MsgpackScanGlobalState &gstate,
                                               idx_t &buffer_index) {
  auto &file_handle = current_reader->GetFileHandle();

  idx_t request_size = gstate.buffer_capacity - prev_buffer_remainder;
  idx_t read_position;
  idx_t read_size;

  {
    lock_guard<mutex> reader_guard(current_reader->lock);
    buffer_index = current_reader->GetBufferIndex();

    read_size = file_handle.GetPositionAndSize(read_position, request_size);
    is_last = read_size < request_size;

    if (read_size == 0 && prev_buffer_remainder != 0) {
      ThrowInvalidAtEndError();
    }

    batch_index = gstate.batch_index++;
  }
  buffer_size = prev_buffer_remainder + read_size;
  if (buffer_size == 0) {
    current_reader->SetBufferLineOrObjectCount(buffer_index, 0);
    return;
  }

  // Now read the file lock-free!
  file_handle.ReadAtPosition(buffer_ptr + prev_buffer_remainder, read_size,
                             read_position, false);
}

void MsgpackScanLocalState::ReadNextBufferNoSeek(MsgpackScanGlobalState &gstate,
                                                 idx_t &buffer_index) {
  idx_t request_size = gstate.buffer_capacity - prev_buffer_remainder;
  idx_t read_size;
  {
    lock_guard<mutex> reader_guard(current_reader->lock);
    buffer_index = current_reader->GetBufferIndex();

    if (current_reader->IsOpen() && !current_reader->IsDone()) {
      read_size = current_reader->GetFileHandle().Read(
          buffer_ptr + prev_buffer_remainder, request_size, false);
      is_last = read_size < request_size;
    } else {
      read_size = 0;
      is_last = false;
    }

    if (read_size == 0 && prev_buffer_remainder != 0) {
      ThrowInvalidAtEndError();
    }

    batch_index = gstate.batch_index++;
  }
  buffer_size = prev_buffer_remainder + read_size;
  if (buffer_size == 0) {
    current_reader->SetBufferLineOrObjectCount(buffer_index, 0);
    return;
  }
}

void MsgpackScanLocalState::ParseNextChunk() {
  auto buffer_offset_before = buffer_offset;

  for (; scan_count < STANDARD_VECTOR_SIZE && buffer_offset < buffer_size;
       scan_count++) {
    try {
      values[scan_count] = ParseMsgpack();
    } catch (const msgpack::insufficient_bytes &e) {
      // incomplete msgpack object
      if (!is_last) {
        idx_t remaining = buffer_size - buffer_offset;
        // carry over remainder
        memcpy(reconstruct_buffer.get(), buffer_ptr + buffer_offset, remaining);
        prev_buffer_remainder = remaining;
      }
      break;
    }
  }

  total_read_size += buffer_offset - buffer_offset_before;
  total_tuple_count += scan_count;
}

msgpack::object_handle MsgpackScanLocalState::ParseMsgpack() {
  msgpack::object_handle unpacked =
      msgpack::unpack(buffer_ptr, buffer_size, buffer_offset);
  if (unpacked.get().type != msgpack::type::MAP) {
    throw InvalidInputException("only map can be scanned");
  }
  return unpacked;
}

void MsgpackScanLocalState::ThrowInvalidAtEndError() {
  throw InvalidInputException(
      "Invalid Msgpack detected at the end of file \"%s\".",
      current_reader->GetFileName());
}

bool MsgpackScanLocalState::IsParallel(MsgpackScanGlobalState &gstate) const {
  // More files than threads, just parallelize over the files
  return bind_data.files.size() < gstate.system_threads;
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
} // namespace duckdb
