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

#include "stage_connection.h"

#include <algorithm>
#include <climits>
#include <cstdint>

#include "error.h"
#include "result_helper.h"

namespace adbchg {

std::unique_ptr<StageConnection> StageConnection::CreateFromPGconn(PGconn* conn) {
  return std::make_unique<PgStageConnection>(conn);
}

PgStageConnection::PgStageConnection(PGconn* conn) : conn_(conn) {}

Status PgStageConnection::ExecuteCommand(const std::string& sql) {
  std::lock_guard<std::mutex> lock(mutex_);
  adbcpq::PqResultHelper result_helper(conn_, sql);
  return result_helper.Execute();
}

Status PgStageConnection::ExecuteCopy(const std::string& copy_sql, const char* data,
                                      int64_t len, std::string* error_msg) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Step 1: Begin COPY
  PGresult* result = PQexec(conn_, copy_sql.c_str());
  ExecStatusType begin_status = PQresultStatus(result);
  if (begin_status != PGRES_COPY_IN) {
    auto status = adbcpq::MakeStatus(result, "[hologres] COPY to stage failed: {}",
                                     PQerrorMessage(conn_));
    if (error_msg) *error_msg = PQerrorMessage(conn_);
    PQclear(result);
    return status;
  }
  PQclear(result);

  // Step 2: Send data (may need chunking for large data)
  int64_t remaining = len;
  const char* ptr = data;
  while (remaining > 0) {
    int to_send = static_cast<int>(std::min<int64_t>(remaining, INT_MAX));
    int send_result = PQputCopyData(conn_, ptr, to_send);
    if (send_result <= 0) {
      if (error_msg) *error_msg = PQerrorMessage(conn_);
      return Status::IO("[hologres] Failed to send data to stage: ",
                        PQerrorMessage(conn_));
    }
    ptr += to_send;
    remaining -= to_send;
  }

  // Step 3: End COPY
  int end_result = PQputCopyEnd(conn_, nullptr);
  if (end_result <= 0) {
    if (error_msg) *error_msg = PQerrorMessage(conn_);
    return Status::IO("[hologres] Failed to end COPY to stage: ",
                      PQerrorMessage(conn_));
  }

  result = PQgetResult(conn_);
  ExecStatusType pg_status = PQresultStatus(result);
  if (pg_status != PGRES_COMMAND_OK) {
    auto status = adbcpq::MakeStatus(result, "[hologres] Stage COPY failed: {}",
                                     PQerrorMessage(conn_));
    if (error_msg) *error_msg = PQerrorMessage(conn_);
    PQclear(result);
    return status;
  }
  PQclear(result);

  return Status::Ok();
}

Status PgStageConnection::BeginCopyIn(const std::string& copy_sql) {
  std::lock_guard<std::mutex> lock(mutex_);
  PGresult* result = PQexec(conn_, copy_sql.c_str());
  if (PQresultStatus(result) != PGRES_COPY_IN) {
    auto status = adbcpq::MakeStatus(result, "[hologres] COPY to stage failed: {}",
                                     PQerrorMessage(conn_));
    PQclear(result);
    return status;
  }
  PQclear(result);
  return Status::Ok();
}

Status PgStageConnection::SendCopyData(const char* data, int len) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (PQputCopyData(conn_, data, len) <= 0) {
    return Status::IO("[hologres] Failed to send data to stage: ",
                      PQerrorMessage(conn_));
  }
  return Status::Ok();
}

Status PgStageConnection::EndCopyIn(std::string* error_msg) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (PQputCopyEnd(conn_, nullptr) <= 0) {
    if (error_msg) {
      *error_msg = PQerrorMessage(conn_);
    }
    return Status::IO("[hologres] Failed to end COPY to stage: ",
                      PQerrorMessage(conn_));
  }

  PGresult* result = PQgetResult(conn_);
  ExecStatusType pg_status = PQresultStatus(result);
  if (pg_status != PGRES_COMMAND_OK) {
    auto status = adbcpq::MakeStatus(result, "[hologres] Stage COPY failed: {}",
                                     PQerrorMessage(conn_));
    if (error_msg) {
      *error_msg = PQerrorMessage(conn_);
    }
    PQclear(result);
    return status;
  }
  PQclear(result);
  return Status::Ok();
}

}  // namespace adbchg
