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

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <nanoarrow/nanoarrow.hpp>
#include <nanoarrow/nanoarrow_ipc.hpp>

#include "stage_connection.h"

namespace adbchg {

/// ON_CONFLICT behavior for Hologres COPY / Stage ingestion
enum class OnConflictMode {
  kNone,    // Default: error on conflict
  kIgnore,  // Skip conflicting rows
  kUpdate,  // Update conflicting rows
};

/// Configuration for Hologres stage-based ingestion
static constexpr int64_t kDefaultTtlSeconds = 7200;
static constexpr int64_t kDefaultBatchSize = 8192;
static constexpr int64_t kDefaultTargetFileSize = 10 * 1024 * 1024;
static constexpr int kDefaultUploadConcurrency = 4;

struct HologresStageConfig {
  std::string stage_name;
  std::string group_name = "default_group";
  int64_t ttl_seconds = kDefaultTtlSeconds;
  int64_t batch_size = kDefaultBatchSize;
  int64_t target_file_size = kDefaultTargetFileSize;
  int upload_concurrency = kDefaultUploadConcurrency;
};

/// Memory buffer containing serialized Arrow IPC data
struct StageBuffer {
  std::vector<uint8_t> data;
  std::string file_name;
  int64_t rows = 0;
};

/// Thread-safe buffer queue for parallel uploads
class BufferQueue {
 public:
  BufferQueue() = default;
  ~BufferQueue() = default;

  void Push(std::unique_ptr<StageBuffer> buffer);
  std::unique_ptr<StageBuffer> Pop();
  void Close();
  bool IsClosed() const;

 private:
  std::queue<std::unique_ptr<StageBuffer>> queue_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool closed_ = false;
};

/// Hologres Stage writer for Arrow-based bulk ingestion.
///
/// Workflow:
/// 1. CreateStage() - Create the internal stage
/// 2. WriteToStage() - Upload Arrow data as multiple .arrow files
/// 3. InsertFromStage() - Execute INSERT...SELECT FROM EXTERNAL_FILES
/// 4. DropStage() - Clean up the stage
class HologresStageWriter {
 public:
  HologresStageWriter(StageConnection* conn, const HologresStageConfig& config);
  ~HologresStageWriter();

  HologresStageWriter(const HologresStageWriter&) = delete;
  HologresStageWriter& operator=(const HologresStageWriter&) = delete;

  Status CreateStage();
  Status WriteToStage(struct ArrowArrayStream* stream, int64_t* rows_affected);
  Status InsertFromStage(const std::string& target_table,
                         const std::string& escaped_field_list,
                         const std::string& escaped_type_list,
                         OnConflictMode on_conflict,
                         const std::string& pk_columns = "",
                         const std::string& escaped_select_list = "");
  Status DropStage();

  const std::string& stage_name() const { return config_.stage_name; }
  static std::string GenerateStageName();
  static Status SerializeArrayToIpcBuffer(const struct ArrowSchema* schema,
                                          const struct ArrowArray* array,
                                          std::vector<uint8_t>* out_buffer,
                                          struct ArrowError* error);

  std::string FormatFileName(int index) const;
  std::string BuildOnConflictClause(OnConflictMode mode,
                                    const std::string& pk_columns) const;

 private:
  StageConnection* conn_;
  HologresStageConfig config_;
  std::atomic<int> file_index_{0};
  std::atomic<int64_t> total_rows_{0};
  bool stage_created_ = false;

  std::unique_ptr<BufferQueue> buffer_queue_;
  std::vector<std::thread> upload_threads_;
  std::atomic<bool> error_flag_{false};
  std::string error_message_;
  std::mutex error_mutex_;

  void UploadThread();
  void JoinUploadThreads();
  Status UploadBufferToStage(const StageBuffer& buffer);

  friend class HologresStageWriterTestHelper;
};

}  // namespace adbchg
