// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifdef _WIN32
#include <winsock2.h>
#endif

#include "arrow_copy_reader.h"

#include <cerrno>
#include <cstring>
#include <memory>

#include <lz4.h>

#include "postgres_util.h"

namespace adbchg {

// PG binary COPY signature: PGCOPY\n\xff\r\n\0
static const char kPgCopySignature[] = "PGCOPY\n\xff\r\n";
static constexpr int kPgCopySignatureLen = 11;

ArrowCopyReader::~ArrowCopyReader() { Release(); }

void ArrowCopyReader::Release() {
  if (pgbuf_) {
    PQfreemem(pgbuf_);
    pgbuf_ = nullptr;
  }
  if (result_) {
    PQclear(result_);
    result_ = nullptr;
  }
  buffer_.clear();
  cursor_ = 0;
}

// ---------------------------------------------------------------------------
// Buffer management
// ---------------------------------------------------------------------------

int ArrowCopyReader::FillBuffer() {
  if (copy_done_) return ENODATA;

  if (pgbuf_) {
    PQfreemem(pgbuf_);
    pgbuf_ = nullptr;
  }

  int bytes_read = PQgetCopyData(conn_, &pgbuf_, 0);
  if (bytes_read == -2) {
    InternalAdbcSetError(&error_, "[hologres] COPY OUT error: %s",
                         PQerrorMessage(conn_));
    status_ = ADBC_STATUS_IO;
    return EIO;
  }
  if (bytes_read == -1) {
    copy_done_ = true;
    return ENODATA;
  }

  buffer_.insert(buffer_.end(), reinterpret_cast<uint8_t*>(pgbuf_),
                 reinterpret_cast<uint8_t*>(pgbuf_) + bytes_read);
  return NANOARROW_OK;
}

int ArrowCopyReader::EnsureAvailable(int64_t n) {
  while (static_cast<int64_t>(buffer_.size()) - cursor_ < n) {
    int rc = FillBuffer();
    if (rc != NANOARROW_OK) return rc;
  }
  return NANOARROW_OK;
}

void ArrowCopyReader::Consume(uint8_t* dest, int64_t n) {
  std::memcpy(dest, buffer_.data() + cursor_, n);
  cursor_ += n;
}

void ArrowCopyReader::SkipBytes(int64_t n) { cursor_ += n; }

void ArrowCopyReader::Compact() {
  if (cursor_ > 0) {
    buffer_.erase(buffer_.begin(), buffer_.begin() + cursor_);
    cursor_ = 0;
  }
}

// ---------------------------------------------------------------------------
// PG COPY header parsing
// ---------------------------------------------------------------------------

int ArrowCopyReader::ReadPgCopyHeader() {
  // Read signature (11 bytes)
  int rc = EnsureAvailable(kPgCopySignatureLen);
  if (rc != NANOARROW_OK) {
    ArrowErrorSet(&na_error_, "Unexpected end of COPY data while reading header");
    return rc;
  }

  if (std::memcmp(buffer_.data() + cursor_, kPgCopySignature, kPgCopySignatureLen) != 0) {
    ArrowErrorSet(&na_error_, "Invalid PG COPY binary signature");
    return EINVAL;
  }
  cursor_ += kPgCopySignatureLen;

  // Read flags (4 bytes) + extension area length (4 bytes)
  rc = EnsureAvailable(8);
  if (rc != NANOARROW_OK) return rc;

  char tmp[4];
  Consume(reinterpret_cast<uint8_t*>(tmp), 4);
  uint32_t flags = adbcpq::LoadNetworkUInt32(tmp);
  (void)flags;

  Consume(reinterpret_cast<uint8_t*>(tmp), 4);
  uint32_t ext_len = adbcpq::LoadNetworkUInt32(tmp);

  // Skip extension area
  if (ext_len > 0) {
    rc = EnsureAvailable(ext_len);
    if (rc != NANOARROW_OK) return rc;
    SkipBytes(ext_len);
  }

  Compact();
  header_read_ = true;
  return NANOARROW_OK;
}

// ---------------------------------------------------------------------------
// IPC blob decoding
// ---------------------------------------------------------------------------

int ArrowCopyReader::DecodeIpcBlob(const uint8_t* data, int64_t len,
                                   struct ArrowArray* out) {
  // Hologres uses pre-1.0 Arrow IPC format (no continuation tokens).
  // nanoarrow's flatcc verifier rejects the RecordBatch messages from Hologres,
  // so we use the low-level ArrowIpcDecoder API directly (DecodeHeader instead
  // of VerifyHeader) to bypass flatcc verification while still correctly
  // parsing the message.
  nanoarrow::ipc::UniqueDecoder decoder;
  int rc = ArrowIpcDecoderInit(decoder.get());
  if (rc != NANOARROW_OK) {
    ArrowErrorSet(&na_error_, "Failed to initialize IPC decoder");
    return rc;
  }

  int64_t pos = 0;

  while (pos < len) {
    if (pos + 4 > len) break;

    // Check for end-of-stream (4 zero bytes)
    uint32_t first4;
    std::memcpy(&first4, data + pos, 4);
    if (first4 == 0) break;

    // Peek at message header to determine sizes
    struct ArrowBufferView peek_view;
    peek_view.data.as_uint8 = data + pos;
    peek_view.size_bytes = len - pos;

    int32_t prefix_size_bytes;
    rc = ArrowIpcDecoderPeekHeader(decoder.get(), peek_view,
                                   &prefix_size_bytes, &na_error_);
    if (rc == ENODATA) break;
    if (rc != NANOARROW_OK) return rc;

    int64_t header_size = decoder->header_size_bytes;

    // Decode header without flatcc verification
    struct ArrowBufferView header_view;
    header_view.data.as_uint8 = data + pos;
    header_view.size_bytes = header_size;

    rc = ArrowIpcDecoderDecodeHeader(decoder.get(), header_view, &na_error_);
    if (rc != NANOARROW_OK) return rc;

    // Body starts after padded header
    int64_t header_padded = (header_size + 7) & ~7;
    int64_t body_offset = pos + header_padded;
    int64_t body_len = decoder->body_size_bytes;

    if (decoder->message_type == NANOARROW_IPC_MESSAGE_TYPE_SCHEMA) {
      nanoarrow::UniqueSchema batch_schema;
      rc = ArrowIpcDecoderDecodeSchema(decoder.get(), batch_schema.get(), &na_error_);
      if (rc != NANOARROW_OK) return rc;

      rc = ArrowIpcDecoderSetSchema(decoder.get(), batch_schema.get(), &na_error_);
      if (rc != NANOARROW_OK) return rc;

      if (!schema_set_) {
        rc = ArrowSchemaDeepCopy(batch_schema.get(), schema_.get());
        if (rc != NANOARROW_OK) {
          ArrowErrorSet(&na_error_, "Failed to copy schema");
          return rc;
        }
        schema_set_ = true;
      }

    } else if (decoder->message_type == NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH) {
      // Hologres declares LZ4_FRAME body compression in RecordBatch metadata
      // but the buffers are NOT actually per-buffer compressed (no 8-byte
      // uncompressed-length prefix). Override to NONE to prevent nanoarrow
      // from attempting per-buffer decompression.
      decoder->codec = NANOARROW_IPC_COMPRESSION_TYPE_NONE;

      struct ArrowBufferView body_view;
      body_view.data.as_uint8 = data + body_offset;
      body_view.size_bytes = body_len;

      return ArrowIpcDecoderDecodeArray(decoder.get(), body_view, -1, out,
                                        NANOARROW_VALIDATION_LEVEL_FULL, &na_error_);
    }

    pos = body_offset + body_len;
  }

  out->release = nullptr;
  return NANOARROW_OK;
}

// ---------------------------------------------------------------------------
// LZ4 decompression
// ---------------------------------------------------------------------------

int ArrowCopyReader::DecompressLz4(const uint8_t* src, int32_t src_len,
                                   int32_t decompressed_size) {
  decompress_buf_.resize(decompressed_size);
  int result = LZ4_decompress_safe(reinterpret_cast<const char*>(src),
                                   reinterpret_cast<char*>(decompress_buf_.data()),
                                   src_len, decompressed_size);
  if (result < 0) {
    ArrowErrorSet(&na_error_, "LZ4 decompression failed (return code %d)", result);
    return EINVAL;
  }
  return NANOARROW_OK;
}

// ---------------------------------------------------------------------------
// Read next batch from PG COPY stream
// ---------------------------------------------------------------------------

int ArrowCopyReader::ReadNextBatch(struct ArrowArray* out) {
  // Read field count (2 bytes, big-endian int16)
  int rc = EnsureAvailable(2);
  if (rc != NANOARROW_OK) {
    if (rc == ENODATA) {
      is_finished_ = true;
      out->release = nullptr;
      return NANOARROW_OK;
    }
    return rc;
  }

  char hdr[4];
  Consume(reinterpret_cast<uint8_t*>(hdr), 2);
  int16_t field_count = adbcpq::LoadNetworkInt16(hdr);

  if (field_count == -1) {
    // PG COPY trailer
    is_finished_ = true;
    out->release = nullptr;
    Compact();
    return NANOARROW_OK;
  }

  if (field_count != 1) {
    ArrowErrorSet(&na_error_,
                  "Expected field_count=1 in Arrow COPY row, got %d", field_count);
    return EINVAL;
  }

  // Read field length (4 bytes, big-endian int32)
  rc = EnsureAvailable(4);
  if (rc != NANOARROW_OK) return rc;

  Consume(reinterpret_cast<uint8_t*>(hdr), 4);
  int32_t field_length = adbcpq::LoadNetworkInt32(hdr);

  if (field_length <= 0) {
    ArrowErrorSet(&na_error_, "Invalid field length %d in Arrow COPY row", field_length);
    return EINVAL;
  }

  // Read the full IPC blob
  rc = EnsureAvailable(field_length);
  if (rc != NANOARROW_OK) return rc;

  const uint8_t* blob_data = buffer_.data() + cursor_;

  if (use_lz4_) {
    // First 4 bytes: decompressed size (big-endian)
    if (field_length < 4) {
      ArrowErrorSet(&na_error_, "Arrow LZ4 blob too short (%d bytes)", field_length);
      return EINVAL;
    }
    int32_t decompressed_size = adbcpq::LoadNetworkInt32(
        reinterpret_cast<const char*>(blob_data));
    int32_t compressed_len = field_length - 4;

    rc = DecompressLz4(blob_data + 4, compressed_len, decompressed_size);
    cursor_ += field_length;
    Compact();
    if (rc != NANOARROW_OK) return rc;

    return DecodeIpcBlob(decompress_buf_.data(), decompressed_size, out);
  }

  // Non-compressed: decode directly from buffer
  rc = DecodeIpcBlob(blob_data, field_length, out);
  cursor_ += field_length;
  Compact();
  return rc;
}

// ---------------------------------------------------------------------------
// ArrowArrayStream interface
// ---------------------------------------------------------------------------

int ArrowCopyReader::GetSchema(struct ArrowSchema* out) {
  if (!schema_set_) {
    // Schema is embedded in the first IPC blob.  Pre-read it now so that
    // get_schema() is available before the first get_next() call.
    if (!header_read_) {
      int rc = ReadPgCopyHeader();
      if (rc != NANOARROW_OK) {
        status_ = ADBC_STATUS_IO;
        return rc;
      }
    }

    struct ArrowArray batch;
    batch.release = nullptr;
    int rc = ReadNextBatch(&batch);
    if (rc != NANOARROW_OK) {
      status_ = ADBC_STATUS_IO;
      return rc;
    }

    if (!schema_set_) {
      ArrowErrorSet(&na_error_, "Schema not available after reading first batch");
      if (batch.release) batch.release(&batch);
      return EINVAL;
    }

    // Cache the first batch so GetNext() can return it later
    if (batch.release) {
      ArrowArrayMove(&batch, first_batch_.get());
      first_batch_cached_ = true;
    }
  }
  return ArrowSchemaDeepCopy(schema_.get(), out);
}

int ArrowCopyReader::GetNext(struct ArrowArray* out) {
  if (first_batch_cached_) {
    ArrowArrayMove(first_batch_.get(), out);
    first_batch_cached_ = false;
    return NANOARROW_OK;
  }

  if (is_finished_) {
    out->release = nullptr;
    return NANOARROW_OK;
  }

  if (!header_read_) {
    int rc = ReadPgCopyHeader();
    if (rc != NANOARROW_OK) {
      status_ = ADBC_STATUS_IO;
      return rc;
    }
  }

  int rc = ReadNextBatch(out);
  if (rc != NANOARROW_OK) {
    status_ = ADBC_STATUS_IO;
  }
  return rc;
}

// ---------------------------------------------------------------------------
// ArrowArrayStream trampolines (same pattern as TupleReader)
// ---------------------------------------------------------------------------

int ArrowCopyReader::GetSchemaTrampoline(struct ArrowArrayStream* self,
                                         struct ArrowSchema* out) {
  if (!self || !self->private_data) return EINVAL;
  auto weak = static_cast<std::weak_ptr<ArrowCopyReader>*>(self->private_data);
  auto shared = weak->lock();
  if (!shared) return EINVAL;
  return shared->GetSchema(out);
}

int ArrowCopyReader::GetNextTrampoline(struct ArrowArrayStream* self,
                                       struct ArrowArray* out) {
  if (!self || !self->private_data) return EINVAL;
  auto weak = static_cast<std::weak_ptr<ArrowCopyReader>*>(self->private_data);
  auto shared = weak->lock();
  if (!shared) return EINVAL;
  return shared->GetNext(out);
}

const char* ArrowCopyReader::GetLastErrorTrampoline(struct ArrowArrayStream* self) {
  if (!self || !self->private_data) return nullptr;
  auto weak = static_cast<std::weak_ptr<ArrowCopyReader>*>(self->private_data);
  auto shared = weak->lock();
  if (!shared) return nullptr;
  return shared->last_error();
}

void ArrowCopyReader::ReleaseTrampoline(struct ArrowArrayStream* self) {
  if (!self) return;
  if (self->private_data) {
    delete static_cast<std::weak_ptr<ArrowCopyReader>*>(self->private_data);
    self->private_data = nullptr;
  }
  self->release = nullptr;
}

void ArrowCopyReader::ExportTo(struct ArrowArrayStream* stream) {
  stream->get_schema = GetSchemaTrampoline;
  stream->get_next = GetNextTrampoline;
  stream->get_last_error = GetLastErrorTrampoline;
  stream->release = ReleaseTrampoline;
  stream->private_data = new std::weak_ptr<ArrowCopyReader>(shared_from_this());
}

const struct AdbcError* ArrowCopyReader::ErrorFromArrayStream(
    struct ArrowArrayStream* stream, AdbcStatusCode* status) {
  if (!stream || !stream->private_data) return nullptr;
  auto weak = static_cast<std::weak_ptr<ArrowCopyReader>*>(stream->private_data);
  auto shared = weak->lock();
  if (!shared) return nullptr;
  *status = shared->status_;
  return &shared->error_;
}

}  // namespace adbchg
