#include "buffered_msgpack_reader.hpp"

namespace duckdb {

MsgpackBufferHandle::MsgpackBufferHandle(idx_t buffer_index_p, idx_t readers_p,
                                         AllocatedData &&buffer_p,
                                         idx_t buffer_size_p)
    : buffer_index(buffer_index_p), readers(readers_p),
      buffer(std::move(buffer_p)), buffer_size(buffer_size_p) {}

MsgpackFileHandle::MsgpackFileHandle(unique_ptr<FileHandle> file_handle_p,
                                     Allocator &allocator_p)
    : file_handle(std::move(file_handle_p)), allocator(allocator_p),
      can_seek(file_handle->CanSeek()),
      plain_file_source(file_handle->OnDiskFile() && can_seek),
      file_size(file_handle->GetFileSize()), read_position(0),
      requested_reads(0), actual_reads(0), cached_size(0) {}

bool MsgpackFileHandle::IsOpen() const { return file_handle != nullptr; }

void MsgpackFileHandle::Close() {
  if (file_handle) {
    file_handle->Close();
    file_handle = nullptr;
  }
  cached_buffers.clear();
}

idx_t MsgpackFileHandle::FileSize() const { return file_size; }

idx_t MsgpackFileHandle::Remaining() const { return file_size - read_position; }

bool MsgpackFileHandle::CanSeek() const { return can_seek; }

void MsgpackFileHandle::Seek(idx_t position) { file_handle->Seek(position); }

idx_t MsgpackFileHandle::GetPositionAndSize(idx_t &position,
                                            idx_t requested_size) {
  D_ASSERT(requested_size != 0);
  position = read_position;
  auto actual_size = MinValue<idx_t>(requested_size, Remaining());
  read_position += actual_size;
  if (actual_size != 0) {
    requested_reads++;
  }

  return actual_size;
}

void MsgpackFileHandle::ReadAtPosition(char *pointer, idx_t size,
                                       idx_t position, bool sample_run) {
  D_ASSERT(size != 0);
  if (plain_file_source) {
    file_handle->Read(pointer, size, position);
    actual_reads++;
    return;
  }

  if (sample_run) { // Cache the buffer
    file_handle->Read(pointer, size, position);
    actual_reads++;
    cached_buffers.emplace_back(allocator.Allocate(size));
    memcpy(cached_buffers.back().get(), pointer, size);
    cached_size += size;
    return;
  }

  if (!cached_buffers.empty() || position < cached_size) {
    ReadFromCache(pointer, size, position);
    actual_reads++;
  }
  if (size != 0) {
    file_handle->Read(pointer, size, position);
    actual_reads++;
  }
}

idx_t MsgpackFileHandle::Read(char *pointer, idx_t requested_size,
                              bool sample_run) {
  D_ASSERT(requested_size != 0);
  if (plain_file_source) {
    auto actual_size = ReadInternal(pointer, requested_size);
    read_position += actual_size;
    return actual_size;
  }

  if (sample_run) { // Cache the buffer
    auto actual_size = ReadInternal(pointer, requested_size);
    if (actual_size > 0) {
      cached_buffers.emplace_back(allocator.Allocate(actual_size));
      memcpy(cached_buffers.back().get(), pointer, actual_size);
    }
    cached_size += actual_size;
    read_position += actual_size;
    return actual_size;
  }

  idx_t actual_size = 0;
  if (!cached_buffers.empty() || read_position < cached_size) {
    actual_size += ReadFromCache(pointer, requested_size, read_position);
  }
  if (requested_size != 0) {
    actual_size += ReadInternal(pointer, requested_size);
  }
  return actual_size;
}

idx_t MsgpackFileHandle::ReadFromCache(char *&pointer, idx_t &size,
                                       idx_t &position) {
  idx_t read_size = 0;
  idx_t total_offset = 0;

  idx_t cached_buffer_idx;
  for (cached_buffer_idx = 0; cached_buffer_idx < cached_buffers.size();
       cached_buffer_idx++) {
    auto &cached_buffer = cached_buffers[cached_buffer_idx];
    if (size == 0) {
      break;
    }
    if (position < total_offset + cached_buffer.GetSize()) {
      idx_t within_buffer_offset = position - total_offset;
      idx_t copy_size =
          MinValue<idx_t>(size, cached_buffer.GetSize() - within_buffer_offset);
      memcpy(pointer, cached_buffer.get() + within_buffer_offset, copy_size);

      read_size += copy_size;
      pointer += copy_size;
      size -= copy_size;
      position += copy_size;
    }
    total_offset += cached_buffer.GetSize();
  }

  return read_size;
}

idx_t MsgpackFileHandle::ReadInternal(char *pointer,
                                      const idx_t requested_size) {
  // Deal with reading from pipes
  idx_t total_read_size = 0;
  while (total_read_size < requested_size) {
    auto read_size = file_handle->Read(pointer + total_read_size,
                                       requested_size - total_read_size);
    if (read_size == 0) {
      break;
    }
    total_read_size += read_size;
  }
  return total_read_size;
}

BufferedMsgpackReader::BufferedMsgpackReader(
    ClientContext &context, BufferedMsgpackReaderOptions options_p,
    string file_name_p)
    : context(context), options(options_p), file_name(std::move(file_name_p)),
      buffer_index(0) {}

void BufferedMsgpackReader::OpenMsgpackFile() {
  D_ASSERT(!IsDone());
  lock_guard<mutex> guard(lock);
  auto &file_system = FileSystem::GetFileSystem(context);
  auto regular_file_handle =
      file_system.OpenFile(file_name.c_str(), FileFlags::FILE_FLAGS_READ,
                           FileLockType::NO_LOCK, options.compression);
  file_handle = make_uniq<MsgpackFileHandle>(std::move(regular_file_handle),
                                             BufferAllocator::Get(context));
}

void BufferedMsgpackReader::CloseMsgpackFile() {
  while (true) {
    lock_guard<mutex> guard(lock);
    if (file_handle->RequestedReadsComplete()) {
      file_handle->Close();
      break;
    }
  }
}

bool BufferedMsgpackReader::IsOpen() const { return file_handle != nullptr; }

bool BufferedMsgpackReader::IsDone() const {
  if (file_handle) {
    return !file_handle->IsOpen();
  }
  return false;
}

BufferedMsgpackReaderOptions &BufferedMsgpackReader::GetOptions() {
  return options;
}

const BufferedMsgpackReaderOptions &BufferedMsgpackReader::GetOptions() const {
  return options;
}

const string &BufferedMsgpackReader::GetFileName() const { return file_name; }

MsgpackFileHandle &BufferedMsgpackReader::GetFileHandle() const {
  return *file_handle;
}

void BufferedMsgpackReader::InsertBuffer(
    idx_t buffer_idx, unique_ptr<MsgpackBufferHandle> &&buffer) {
  lock_guard<mutex> guard(lock);
  buffer_map.insert(make_pair(buffer_idx, std::move(buffer)));
}

MsgpackBufferHandle *BufferedMsgpackReader::GetBuffer(idx_t buffer_idx) {
  lock_guard<mutex> guard(lock);
  auto it = buffer_map.find(buffer_idx);
  return it == buffer_map.end() ? nullptr : it->second.get();
}

AllocatedData BufferedMsgpackReader::RemoveBuffer(idx_t buffer_idx) {
  lock_guard<mutex> guard(lock);
  auto it = buffer_map.find(buffer_idx);
  D_ASSERT(it != buffer_map.end());
  auto result = std::move(it->second->buffer);
  buffer_map.erase(it);
  return result;
}

idx_t BufferedMsgpackReader::GetBufferIndex() {
  buffer_line_or_object_counts.push_back(-1);
  return buffer_index++;
}

void BufferedMsgpackReader::SetBufferLineOrObjectCount(idx_t index,
                                                       idx_t count) {
  lock_guard<mutex> guard(lock);
  buffer_line_or_object_counts[index] = count;
}

idx_t BufferedMsgpackReader::GetLineNumber(idx_t buf_index,
                                           idx_t line_or_object_in_buf) {
  while (true) {
    lock_guard<mutex> guard(lock);
    idx_t line = line_or_object_in_buf;
    bool can_throw = true;
    for (idx_t b_idx = 0; b_idx < buf_index; b_idx++) {
      if (buffer_line_or_object_counts[b_idx] == -1) {
        can_throw = false;
        break;
      } else {
        line += buffer_line_or_object_counts[b_idx];
      }
    }
    if (!can_throw) {
      continue;
    }
    // SQL uses 1-based indexing so I guess we will do that in our exception
    // here as well
    return line + 1;
  }
}

double BufferedMsgpackReader::GetProgress() const {
  if (IsOpen()) {
    return 100.0 - 100.0 * double(file_handle->Remaining()) /
                       double(file_handle->FileSize());
  } else {
    return 0;
  }
}

void BufferedMsgpackReader::Reset() {
  buffer_index = 0;
  buffer_map.clear();
  buffer_line_or_object_counts.clear();

  if (!file_handle) {
    return;
  }

  if (file_handle->CanSeek()) {
    file_handle->Seek(0);
  } else {
    file_handle->Reset();
  }
  file_handle->Reset();
}

void MsgpackFileHandle::Reset() {
  read_position = 0;
  requested_reads = 0;
  actual_reads = 0;
  if (plain_file_source) {
    file_handle->Reset();
  }
}

bool MsgpackFileHandle::RequestedReadsComplete() {
  return requested_reads == actual_reads;
}

} // namespace duckdb
