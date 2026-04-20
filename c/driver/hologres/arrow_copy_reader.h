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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <libpq-fe.h>
#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow.hpp>
#include <nanoarrow/nanoarrow_ipc.hpp>

#include "driver/common/utils.h"

namespace adbchg {

/// Reads query results from Hologres via COPY TO STDOUT in Arrow IPC format.
///
/// Hologres wraps Arrow IPC streams inside the PostgreSQL binary COPY protocol:
///   [19-byte PG COPY header]
///   [field_count=1, field_length=N, Arrow IPC blob (N bytes)]  <-- per batch
///   [field_count=-1]                                           <-- trailer
///
/// For arrow_lz4, each IPC blob is prefixed with a 4-byte decompressed size
/// followed by LZ4 block-compressed data.
class ArrowCopyReader final : public std::enable_shared_from_this<ArrowCopyReader> {
 public:
  ArrowCopyReader(PGconn* conn, bool use_lz4)
      : status_(ADBC_STATUS_OK),
        error_(ADBC_ERROR_INIT),
        conn_(conn),
        result_(nullptr),
        pgbuf_(nullptr),
        use_lz4_(use_lz4),
        header_read_(false),
        is_finished_(false),
        schema_set_(false) {
    ArrowErrorInit(&na_error_);
  }

  ~ArrowCopyReader();

  int GetSchema(struct ArrowSchema* out);
  int GetNext(struct ArrowArray* out);
  const char* last_error() const { return error_.message; }
  void Release();
  void ExportTo(struct ArrowArrayStream* stream);

  static const struct AdbcError* ErrorFromArrayStream(struct ArrowArrayStream* stream,
                                                      AdbcStatusCode* status);

 private:
  friend class HologresStatement;

  // Buffer management for PQgetCopyData
  int FillBuffer();
  int EnsureAvailable(int64_t n);
  void Consume(uint8_t* dest, int64_t n);
  void SkipBytes(int64_t n);
  void Compact();

  // PG COPY framing
  int ReadPgCopyHeader();
  int ReadNextBatch(struct ArrowArray* out);

  // IPC decoding
  int DecodeIpcBlob(const uint8_t* data, int64_t len, struct ArrowArray* out);

  // LZ4 decompression
  int DecompressLz4(const uint8_t* src, int32_t src_len, int32_t decompressed_size);

  // ArrowArrayStream trampolines
  static int GetSchemaTrampoline(struct ArrowArrayStream* self, struct ArrowSchema* out);
  static int GetNextTrampoline(struct ArrowArrayStream* self, struct ArrowArray* out);
  static const char* GetLastErrorTrampoline(struct ArrowArrayStream* self);
  static void ReleaseTrampoline(struct ArrowArrayStream* self);

  AdbcStatusCode status_;
  struct AdbcError error_;
  struct ArrowError na_error_;
  PGconn* conn_;
  PGresult* result_;
  char* pgbuf_;
  bool use_lz4_;
  bool header_read_;
  bool is_finished_;

  // Internal buffer for accumulating PQgetCopyData chunks
  std::vector<uint8_t> buffer_;
  int64_t cursor_ = 0;
  bool copy_done_ = false;

  // Schema from first IPC blob
  nanoarrow::UniqueSchema schema_;
  bool schema_set_;

  // First batch pre-read by GetSchema (returned by the first GetNext call)
  nanoarrow::UniqueArray first_batch_;
  bool first_batch_cached_ = false;

  // Reusable LZ4 decompression buffer
  std::vector<uint8_t> decompress_buf_;
};

}  // namespace adbchg
