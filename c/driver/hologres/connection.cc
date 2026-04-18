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

#include "connection.h"

#include "database.h"
#include "driver/common/utils.h"

namespace adbchg {

AdbcStatusCode HologresConnection::Init(struct AdbcDatabase* database,
                                        struct AdbcError* error) {
  if (!database || !database->private_data) {
    InternalAdbcSetError(error, "%s", "[hologres] database is not initialized");
    return ADBC_STATUS_INVALID_STATE;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnection::Release(struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnection::Commit(struct AdbcError* error) {
  InternalAdbcSetError(error, "%s",
                       "[hologres] Hologres does not support transactions");
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::Rollback(struct AdbcError* error) {
  InternalAdbcSetError(error, "%s",
                       "[hologres] Hologres does not support transactions");
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::Cancel(struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::GetInfo(struct AdbcConnection* connection,
                                           const uint32_t* info_codes,
                                           size_t info_codes_length,
                                           struct ArrowArrayStream* stream,
                                           struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::GetObjects(
    struct AdbcConnection* connection, int depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_types,
    const char* column_name, struct ArrowArrayStream* stream, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::GetTableSchema(const char* catalog,
                                                   const char* db_schema,
                                                   const char* table_name,
                                                   struct ArrowSchema* schema,
                                                   struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::GetTableTypes(struct AdbcConnection* connection,
                                                 struct ArrowArrayStream* stream,
                                                 struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::GetStatistics(const char* catalog,
                                                  const char* db_schema,
                                                  const char* table_name,
                                                  bool approximate,
                                                  struct ArrowArrayStream* out,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::GetStatisticNames(struct ArrowArrayStream* out,
                                                     struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::SetOption(const char* key, const char* value,
                                             struct AdbcError* error) {
  if (std::strcmp(key, ADBC_CONNECTION_OPTION_AUTOCOMMIT) == 0) {
    if (std::strcmp(value, ADBC_OPTION_VALUE_DISABLED) == 0) {
      InternalAdbcSetError(
          error, "%s",
          "[hologres] Hologres does not support transactions; "
          "autocommit cannot be disabled");
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }
    return ADBC_STATUS_OK;
  }
  InternalAdbcSetError(error, "[hologres] Unknown connection option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::GetOption(const char* key, char* value,
                                             size_t* length, struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown connection option: %s", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresConnection::SetOptionBytes(const char* key, const uint8_t* value,
                                                   size_t length,
                                                   struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown connection option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::GetOptionBytes(const char* key, uint8_t* value,
                                                   size_t* length,
                                                   struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown connection option: %s", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresConnection::SetOptionDouble(const char* key, double value,
                                                   struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown connection option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::GetOptionDouble(const char* key, double* value,
                                                   struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown connection option: %s", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresConnection::SetOptionInt(const char* key, int64_t value,
                                                struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown connection option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::GetOptionInt(const char* key, int64_t* value,
                                                struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown connection option: %s", key);
  return ADBC_STATUS_NOT_FOUND;
}

}  // namespace adbchg
