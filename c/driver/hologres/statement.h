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

#include <arrow-adbc/adbc.h>

namespace adbchg {

/// Hologres statement (query execution and bulk ingestion).
class HologresStatement {
 public:
  HologresStatement() = default;
  ~HologresStatement() = default;

  // Lifecycle
  AdbcStatusCode New(struct AdbcConnection* connection, struct AdbcError* error);
  AdbcStatusCode Release(struct AdbcError* error);

  // Query
  AdbcStatusCode SetSqlQuery(const char* query, struct AdbcError* error);
  AdbcStatusCode ExecuteQuery(struct ArrowArrayStream* output, int64_t* rows_affected,
                              struct AdbcError* error);
  AdbcStatusCode ExecuteSchema(struct ArrowSchema* schema, struct AdbcError* error);
  AdbcStatusCode Prepare(struct AdbcError* error);
  AdbcStatusCode Cancel(struct AdbcError* error);

  // Bind
  AdbcStatusCode Bind(struct ArrowArray* values, struct ArrowSchema* schema,
                      struct AdbcError* error);
  AdbcStatusCode Bind(struct ArrowArrayStream* stream, struct AdbcError* error);
  AdbcStatusCode GetParameterSchema(struct ArrowSchema* schema, struct AdbcError* error);

  // Options
  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error);
  AdbcStatusCode GetOption(const char* key, char* value, size_t* length,
                           struct AdbcError* error);
  AdbcStatusCode SetOptionBytes(const char* key, const uint8_t* value, size_t length,
                                struct AdbcError* error);
  AdbcStatusCode GetOptionBytes(const char* key, uint8_t* value, size_t* length,
                                struct AdbcError* error);
  AdbcStatusCode SetOptionDouble(const char* key, double value, struct AdbcError* error);
  AdbcStatusCode GetOptionDouble(const char* key, double* value, struct AdbcError* error);
  AdbcStatusCode SetOptionInt(const char* key, int64_t value, struct AdbcError* error);
  AdbcStatusCode GetOptionInt(const char* key, int64_t* value, struct AdbcError* error);
};

}  // namespace adbchg
