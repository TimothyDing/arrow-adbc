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

#include "statement.h"

#include "driver/common/utils.h"

namespace adbchg {

AdbcStatusCode HologresStatement::New(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  if (!connection || !connection->private_data) {
    InternalAdbcSetError(error, "%s", "[hologres] connection is not initialized");
    return ADBC_STATUS_INVALID_STATE;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::Release(struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::SetSqlQuery(const char* query,
                                              struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::ExecuteQuery(struct ArrowArrayStream* output,
                                               int64_t* rows_affected,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::ExecuteSchema(struct ArrowSchema* schema,
                                                struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::Prepare(struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::Cancel(struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::Bind(struct ArrowArray* values,
                                       struct ArrowSchema* schema,
                                       struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::Bind(struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::GetParameterSchema(struct ArrowSchema* schema,
                                                     struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::SetOption(const char* key, const char* value,
                                            struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown statement option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::GetOption(const char* key, char* value,
                                            size_t* length, struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown statement option: %s", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresStatement::SetOptionBytes(const char* key, const uint8_t* value,
                                                 size_t length,
                                                 struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown statement option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::GetOptionBytes(const char* key, uint8_t* value,
                                                 size_t* length,
                                                 struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown statement option: %s", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresStatement::SetOptionDouble(const char* key, double value,
                                                  struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown statement option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::GetOptionDouble(const char* key, double* value,
                                                  struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown statement option: %s", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresStatement::SetOptionInt(const char* key, int64_t value,
                                               struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown statement option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::GetOptionInt(const char* key, int64_t* value,
                                               struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown statement option: %s", key);
  return ADBC_STATUS_NOT_FOUND;
}

}  // namespace adbchg
