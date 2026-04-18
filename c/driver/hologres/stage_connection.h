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

#include <memory>
#include <mutex>
#include <string>

#include <arrow-adbc/adbc.h>
#include <libpq-fe.h>

#include "driver/framework/status.h"

namespace adbchg {
using adbc::driver::Status;

/// Abstract interface for stage database operations.
/// This enables mocking for unit tests without a real database connection.
class StageConnection {
 public:
  virtual ~StageConnection() = default;

  /// Execute a SQL command (e.g., CREATE/DROP STAGE, INSERT).
  virtual Status ExecuteCommand(const std::string& sql) = 0;

  /// Execute an atomic COPY IN operation (thread-safe).
  virtual Status ExecuteCopy(const std::string& copy_sql, const char* data,
                             int64_t len, std::string* error_msg) = 0;

  /// Begin a COPY IN operation.
  virtual Status BeginCopyIn(const std::string& copy_sql) = 0;

  /// Send data during a COPY IN operation.
  virtual Status SendCopyData(const char* data, int len) = 0;

  /// End a COPY IN operation and get the result.
  virtual Status EndCopyIn(std::string* error_msg) = 0;

  /// Create a StageConnection backed by a real PGconn.
  static std::unique_ptr<StageConnection> CreateFromPGconn(PGconn* conn);
};

/// Production implementation of StageConnection using real PGconn.
class PgStageConnection : public StageConnection {
 public:
  explicit PgStageConnection(PGconn* conn);

  Status ExecuteCommand(const std::string& sql) override;
  Status ExecuteCopy(const std::string& copy_sql, const char* data, int64_t len,
                     std::string* error_msg) override;
  Status BeginCopyIn(const std::string& copy_sql) override;
  Status SendCopyData(const char* data, int len) override;
  Status EndCopyIn(std::string* error_msg) override;

 private:
  PGconn* conn_;
  std::mutex mutex_;
};

}  // namespace adbchg
