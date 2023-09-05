#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
struct BufferedMsgpackReaderOptions {
public:
  //! Whether file is compressed or not, and if so which compression type
  FileCompressionType compression = FileCompressionType::AUTO_DETECT;
  //! Multi-file reader options
  MultiFileReaderOptions file_options;

  // public:
  //   void Serialize(FieldWriter &writer) const;
  //   void Deserialize(FieldReader &reader);
};

struct MsgpackBufferHandle {
public:
  MsgpackBufferHandle(idx_t buffer_index, idx_t readers, AllocatedData &&buffer,
                      idx_t buffer_size);

public:
  //! Buffer index (within same file)
  const idx_t buffer_index;

  //! Number of readers for this buffer
  atomic<idx_t> readers;
  //! The buffer
  AllocatedData buffer;
  //! The size of the data in the buffer (can be less than buffer.GetSize())
  const idx_t buffer_size;
};

struct MsgpackFileHandle {
public:
  MsgpackFileHandle(unique_ptr<FileHandle> file_handle, Allocator &allocator);
  bool IsOpen() const;
  void Close();

  idx_t FileSize() const;
  idx_t Remaining() const;

  bool CanSeek() const;
  void Seek(idx_t position);

  idx_t GetPositionAndSize(idx_t &position, idx_t requested_size);
  void ReadAtPosition(char *pointer, idx_t size, idx_t position,
                      bool sample_run);
  idx_t Read(char *pointer, idx_t requested_size, bool sample_run);

  void Reset();
  bool RequestedReadsComplete();

private:
  idx_t ReadFromCache(char *&pointer, idx_t &size, idx_t &position);
  idx_t ReadInternal(char *pointer, const idx_t requested_size);

private:
  //! The Msgpack file handle
  unique_ptr<FileHandle> file_handle;
  Allocator &allocator;

  //! File properties
  const bool can_seek;
  const bool plain_file_source;
  const idx_t file_size;

  //! Read properties
  idx_t read_position;
  idx_t requested_reads;
  atomic<idx_t> actual_reads;

  //! Cached buffers for resetting when reading stream
  vector<AllocatedData> cached_buffers;
  idx_t cached_size;
};

class BufferedMsgpackReader {
public:
  BufferedMsgpackReader(ClientContext &context,
                        BufferedMsgpackReaderOptions options, string file_name);

private:
  ClientContext &context;
  BufferedMsgpackReaderOptions options;

  //! File name
  const string file_name;
  //! File handle
  unique_ptr<MsgpackFileHandle> file_handle;

  //! Next buffer index within the file
  idx_t buffer_index;
  //! Mapping from batch index to currently held buffers
  unordered_map<idx_t, unique_ptr<MsgpackBufferHandle>> buffer_map;

  //! Line count per buffer
  vector<int64_t> buffer_line_or_object_counts;

public:
  mutex lock;

public:
  void OpenMsgpackFile();
  void CloseMsgpackFile();
  bool IsOpen() const;
  bool IsDone() const;

  BufferedMsgpackReaderOptions &GetOptions();
  const BufferedMsgpackReaderOptions &GetOptions() const;

  const string &GetFileName() const;
  MsgpackFileHandle &GetFileHandle() const;

public:
  //! Insert/get/remove buffer (grabs the lock)
  void InsertBuffer(idx_t buffer_idx, unique_ptr<MsgpackBufferHandle> &&buffer);
  MsgpackBufferHandle *GetBuffer(idx_t buffer_idx);
  AllocatedData RemoveBuffer(idx_t buffer_idx);

  //! Get a new buffer index (must hold the lock)
  idx_t GetBufferIndex();
  //! Set line count for a buffer that is done (grabs the lock)
  void SetBufferLineOrObjectCount(idx_t index, idx_t count);
  //! Throws a parse error that mentions the file name and line number
  void ThrowParseError(idx_t buf_index, idx_t line_or_object_in_buf,
                       yyjson_read_err &err, const string &extra = "");
  //! Throws a transform error that mentions the file name and line number
  void ThrowTransformError(idx_t buf_index, idx_t line_or_object_in_buf,
                           const string &error_message);

  double GetProgress() const;
  void Reset();

private:
  idx_t GetLineNumber(idx_t buf_index, idx_t line_or_object_in_buf);
};
} // namespace duckdb
