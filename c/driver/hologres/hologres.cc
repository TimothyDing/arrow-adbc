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

// A libpq-based Hologres driver for ADBC.

#ifdef _WIN32
#include <winsock2.h>
#endif

#include <cstring>
#include <memory>

#include <arrow-adbc/adbc.h>

#include "connection.h"
#include "database.h"
#include "driver/common/utils.h"
#include "driver/framework/status.h"
#include "statement.h"

using adbc::driver::Status;
using adbchg::HologresConnection;
using adbchg::HologresDatabase;
using adbchg::HologresStatement;

// Private wrappers to avoid dynamic linker symbol shadowing.
// See c/driver/postgresql/postgresql.cc for detailed explanation.

// ---------------------------------------------------------------------
// AdbcError

namespace {
int HologresErrorGetDetailCount(const struct AdbcError* error) {
  if (InternalAdbcIsCommonError(error)) {
    return InternalAdbcCommonErrorGetDetailCount(error);
  }
  if (error->vendor_code != ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
    return 0;
  }
  auto error_obj = reinterpret_cast<Status*>(error->private_data);
  return error_obj->CDetailCount();
}

struct AdbcErrorDetail HologresErrorGetDetail(const struct AdbcError* error, int index) {
  if (InternalAdbcIsCommonError(error)) {
    return InternalAdbcCommonErrorGetDetail(error, index);
  }
  auto error_obj = reinterpret_cast<Status*>(error->private_data);
  return error_obj->CDetail(index);
}
}  // namespace

#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)
int AdbcErrorGetDetailCount(const struct AdbcError* error) {
  return HologresErrorGetDetailCount(error);
}

struct AdbcErrorDetail AdbcErrorGetDetail(const struct AdbcError* error, int index) {
  return HologresErrorGetDetail(error, index);
}
#endif  // ADBC_NO_COMMON_ENTRYPOINTS

// ---------------------------------------------------------------------
// AdbcDatabase

namespace {
AdbcStatusCode HologresDatabaseInit(struct AdbcDatabase* database,
                                    struct AdbcError* error) {
  if (!database || !database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresDatabase>*>(database->private_data);
  return (*ptr)->Init(error);
}

AdbcStatusCode HologresDatabaseNew(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  if (!database) {
    InternalAdbcSetError(error, "%s", "[hologres] database must not be null");
    return ADBC_STATUS_INVALID_STATE;
  }
  if (database->private_data) {
    InternalAdbcSetError(error, "%s", "[hologres] database is already initialized");
    return ADBC_STATUS_INVALID_STATE;
  }
  auto impl = std::make_shared<HologresDatabase>();
  database->private_data = new std::shared_ptr<HologresDatabase>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresDatabaseRelease(struct AdbcDatabase* database,
                                       struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresDatabase>*>(database->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  database->private_data = nullptr;
  return status;
}

AdbcStatusCode HologresDatabaseGetOption(struct AdbcDatabase* database, const char* key,
                                         char* value, size_t* length,
                                         struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresDatabase>*>(database->private_data);
  return (*ptr)->GetOption(key, value, length, error);
}

AdbcStatusCode HologresDatabaseGetOptionBytes(struct AdbcDatabase* database,
                                              const char* key, uint8_t* value,
                                              size_t* length, struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresDatabase>*>(database->private_data);
  return (*ptr)->GetOptionBytes(key, value, length, error);
}

AdbcStatusCode HologresDatabaseGetOptionDouble(struct AdbcDatabase* database,
                                               const char* key, double* value,
                                               struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresDatabase>*>(database->private_data);
  return (*ptr)->GetOptionDouble(key, value, error);
}

AdbcStatusCode HologresDatabaseGetOptionInt(struct AdbcDatabase* database,
                                            const char* key, int64_t* value,
                                            struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresDatabase>*>(database->private_data);
  return (*ptr)->GetOptionInt(key, value, error);
}

AdbcStatusCode HologresDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                         const char* value, struct AdbcError* error) {
  if (!database || !database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresDatabase>*>(database->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode HologresDatabaseSetOptionBytes(struct AdbcDatabase* database,
                                              const char* key, const uint8_t* value,
                                              size_t length, struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresDatabase>*>(database->private_data);
  return (*ptr)->SetOptionBytes(key, value, length, error);
}

AdbcStatusCode HologresDatabaseSetOptionDouble(struct AdbcDatabase* database,
                                               const char* key, double value,
                                               struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresDatabase>*>(database->private_data);
  return (*ptr)->SetOptionDouble(key, value, error);
}

AdbcStatusCode HologresDatabaseSetOptionInt(struct AdbcDatabase* database,
                                            const char* key, int64_t value,
                                            struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresDatabase>*>(database->private_data);
  return (*ptr)->SetOptionInt(key, value, error);
}
}  // namespace

#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)
AdbcStatusCode AdbcDatabaseGetOption(struct AdbcDatabase* database, const char* key,
                                     char* value, size_t* length,
                                     struct AdbcError* error) {
  return HologresDatabaseGetOption(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseGetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          uint8_t* value, size_t* length,
                                          struct AdbcError* error) {
  return HologresDatabaseGetOptionBytes(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseGetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t* value, struct AdbcError* error) {
  return HologresDatabaseGetOptionInt(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseGetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double* value, struct AdbcError* error) {
  return HologresDatabaseGetOptionDouble(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error) {
  return HologresDatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  return HologresDatabaseNew(database, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  return HologresDatabaseRelease(database, error);
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  return HologresDatabaseSetOption(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseSetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          const uint8_t* value, size_t length,
                                          struct AdbcError* error) {
  return HologresDatabaseSetOptionBytes(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseSetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t value, struct AdbcError* error) {
  return HologresDatabaseSetOptionInt(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseSetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double value, struct AdbcError* error) {
  return HologresDatabaseSetOptionDouble(database, key, value, error);
}
#endif  // ADBC_NO_COMMON_ENTRYPOINTS

// ---------------------------------------------------------------------
// AdbcConnection

namespace {
AdbcStatusCode HologresConnectionCancel(struct AdbcConnection* connection,
                                        struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->Cancel(error);
}

AdbcStatusCode HologresConnectionCommit(struct AdbcConnection* connection,
                                        struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->Commit(error);
}

AdbcStatusCode HologresConnectionGetInfo(struct AdbcConnection* connection,
                                         const uint32_t* info_codes,
                                         size_t info_codes_length,
                                         struct ArrowArrayStream* stream,
                                         struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->GetInfo(connection, info_codes, info_codes_length, stream, error);
}

AdbcStatusCode HologresConnectionGetObjects(
    struct AdbcConnection* connection, int depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_types,
    const char* column_name, struct ArrowArrayStream* stream, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->GetObjects(connection, depth, catalog, db_schema, table_name,
                            table_types, column_name, stream, error);
}

AdbcStatusCode HologresConnectionGetOption(struct AdbcConnection* connection,
                                           const char* key, char* value, size_t* length,
                                           struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->GetOption(key, value, length, error);
}

AdbcStatusCode HologresConnectionGetOptionBytes(struct AdbcConnection* connection,
                                                const char* key, uint8_t* value,
                                                size_t* length, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->GetOptionBytes(key, value, length, error);
}

AdbcStatusCode HologresConnectionGetOptionDouble(struct AdbcConnection* connection,
                                                 const char* key, double* value,
                                                 struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->GetOptionDouble(key, value, error);
}

AdbcStatusCode HologresConnectionGetOptionInt(struct AdbcConnection* connection,
                                              const char* key, int64_t* value,
                                              struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->GetOptionInt(key, value, error);
}

AdbcStatusCode HologresConnectionGetStatistics(struct AdbcConnection* connection,
                                               const char* catalog,
                                               const char* db_schema,
                                               const char* table_name, char approximate,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->GetStatistics(catalog, db_schema, table_name, approximate == 1, out,
                               error);
}

AdbcStatusCode HologresConnectionGetStatisticNames(struct AdbcConnection* connection,
                                                   struct ArrowArrayStream* out,
                                                   struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->GetStatisticNames(out, error);
}

AdbcStatusCode HologresConnectionGetTableSchema(
    struct AdbcConnection* connection, const char* catalog, const char* db_schema,
    const char* table_name, struct ArrowSchema* schema, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->GetTableSchema(catalog, db_schema, table_name, schema, error);
}

AdbcStatusCode HologresConnectionGetTableTypes(struct AdbcConnection* connection,
                                               struct ArrowArrayStream* stream,
                                               struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->GetTableTypes(connection, stream, error);
}

AdbcStatusCode HologresConnectionInit(struct AdbcConnection* connection,
                                      struct AdbcDatabase* database,
                                      struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->Init(database, error);
}

AdbcStatusCode HologresConnectionNew(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  auto impl = std::make_shared<HologresConnection>();
  connection->private_data = new std::shared_ptr<HologresConnection>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnectionReadPartition(struct AdbcConnection* connection,
                                               const uint8_t* serialized_partition,
                                               size_t serialized_length,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnectionRelease(struct AdbcConnection* connection,
                                         struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  connection->private_data = nullptr;
  return status;
}

AdbcStatusCode HologresConnectionRollback(struct AdbcConnection* connection,
                                          struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->Rollback(error);
}

AdbcStatusCode HologresConnectionSetOption(struct AdbcConnection* connection,
                                           const char* key, const char* value,
                                           struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode HologresConnectionSetOptionBytes(struct AdbcConnection* connection,
                                                const char* key, const uint8_t* value,
                                                size_t length, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->SetOptionBytes(key, value, length, error);
}

AdbcStatusCode HologresConnectionSetOptionDouble(struct AdbcConnection* connection,
                                                 const char* key, double value,
                                                 struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->SetOptionDouble(key, value, error);
}

AdbcStatusCode HologresConnectionSetOptionInt(struct AdbcConnection* connection,
                                              const char* key, int64_t value,
                                              struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  return (*ptr)->SetOptionInt(key, value, error);
}
}  // namespace

#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)
AdbcStatusCode AdbcConnectionCancel(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return HologresConnectionCancel(connection, error);
}

AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return HologresConnectionCommit(connection, error);
}

AdbcStatusCode AdbcConnectionGetInfo(struct AdbcConnection* connection,
                                     const uint32_t* info_codes, size_t info_codes_length,
                                     struct ArrowArrayStream* stream,
                                     struct AdbcError* error) {
  return HologresConnectionGetInfo(connection, info_codes, info_codes_length, stream,
                                   error);
}

AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                        const char* catalog, const char* db_schema,
                                        const char* table_name, const char** table_types,
                                        const char* column_name,
                                        struct ArrowArrayStream* stream,
                                        struct AdbcError* error) {
  return HologresConnectionGetObjects(connection, depth, catalog, db_schema, table_name,
                                      table_types, column_name, stream, error);
}

AdbcStatusCode AdbcConnectionGetOption(struct AdbcConnection* connection, const char* key,
                                       char* value, size_t* length,
                                       struct AdbcError* error) {
  return HologresConnectionGetOption(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionGetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, uint8_t* value,
                                            size_t* length, struct AdbcError* error) {
  return HologresConnectionGetOptionBytes(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionGetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t* value,
                                          struct AdbcError* error) {
  return HologresConnectionGetOptionInt(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double* value,
                                             struct AdbcError* error) {
  return HologresConnectionGetOptionDouble(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetStatistics(struct AdbcConnection* connection,
                                           const char* catalog, const char* db_schema,
                                           const char* table_name, char approximate,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return HologresConnectionGetStatistics(connection, catalog, db_schema, table_name,
                                         approximate, out, error);
}

AdbcStatusCode AdbcConnectionGetStatisticNames(struct AdbcConnection* connection,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* error) {
  return HologresConnectionGetStatisticNames(connection, out, error);
}

AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection* connection,
                                            const char* catalog, const char* db_schema,
                                            const char* table_name,
                                            struct ArrowSchema* schema,
                                            struct AdbcError* error) {
  return HologresConnectionGetTableSchema(connection, catalog, db_schema, table_name,
                                          schema, error);
}

AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct ArrowArrayStream* stream,
                                           struct AdbcError* error) {
  return HologresConnectionGetTableTypes(connection, stream, error);
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  return HologresConnectionInit(connection, database, error);
}

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  return HologresConnectionNew(connection, error);
}

AdbcStatusCode AdbcConnectionReadPartition(struct AdbcConnection* connection,
                                           const uint8_t* serialized_partition,
                                           size_t serialized_length,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return HologresConnectionReadPartition(connection, serialized_partition,
                                         serialized_length, out, error);
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  return HologresConnectionRelease(connection, error);
}

AdbcStatusCode AdbcConnectionRollback(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  return HologresConnectionRollback(connection, error);
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  return HologresConnectionSetOption(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionSetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, const uint8_t* value,
                                            size_t length, struct AdbcError* error) {
  return HologresConnectionSetOptionBytes(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionSetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t value,
                                          struct AdbcError* error) {
  return HologresConnectionSetOptionInt(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionSetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double value,
                                             struct AdbcError* error) {
  return HologresConnectionSetOptionDouble(connection, key, value, error);
}
#endif  // ADBC_NO_COMMON_ENTRYPOINTS

// ---------------------------------------------------------------------
// AdbcStatement

namespace {
AdbcStatusCode HologresStatementBind(struct AdbcStatement* statement,
                                     struct ArrowArray* values,
                                     struct ArrowSchema* schema,
                                     struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->Bind(values, schema, error);
}

AdbcStatusCode HologresStatementBindStream(struct AdbcStatement* statement,
                                           struct ArrowArrayStream* stream,
                                           struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->Bind(stream, error);
}

AdbcStatusCode HologresStatementCancel(struct AdbcStatement* statement,
                                       struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->Cancel(error);
}

AdbcStatusCode HologresStatementExecutePartitions(struct AdbcStatement* statement,
                                                  struct ArrowSchema* schema,
                                                  struct AdbcPartitions* partitions,
                                                  int64_t* rows_affected,
                                                  struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatementExecuteQuery(struct AdbcStatement* statement,
                                             struct ArrowArrayStream* output,
                                             int64_t* rows_affected,
                                             struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->ExecuteQuery(output, rows_affected, error);
}

AdbcStatusCode HologresStatementExecuteSchema(struct AdbcStatement* statement,
                                              struct ArrowSchema* schema,
                                              struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->ExecuteSchema(schema, error);
}

AdbcStatusCode HologresStatementGetOption(struct AdbcStatement* statement,
                                          const char* key, char* value, size_t* length,
                                          struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->GetOption(key, value, length, error);
}

AdbcStatusCode HologresStatementGetOptionBytes(struct AdbcStatement* statement,
                                               const char* key, uint8_t* value,
                                               size_t* length, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->GetOptionBytes(key, value, length, error);
}

AdbcStatusCode HologresStatementGetOptionDouble(struct AdbcStatement* statement,
                                                const char* key, double* value,
                                                struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->GetOptionDouble(key, value, error);
}

AdbcStatusCode HologresStatementGetOptionInt(struct AdbcStatement* statement,
                                             const char* key, int64_t* value,
                                             struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->GetOptionInt(key, value, error);
}

AdbcStatusCode HologresStatementGetParameterSchema(struct AdbcStatement* statement,
                                                   struct ArrowSchema* schema,
                                                   struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->GetParameterSchema(schema, error);
}

AdbcStatusCode HologresStatementNew(struct AdbcConnection* connection,
                                    struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  auto impl = std::make_shared<HologresStatement>();
  statement->private_data = new std::shared_ptr<HologresStatement>(impl);
  return impl->New(connection, error);
}

AdbcStatusCode HologresStatementPrepare(struct AdbcStatement* statement,
                                        struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->Prepare(error);
}

AdbcStatusCode HologresStatementRelease(struct AdbcStatement* statement,
                                        struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  auto status = (*ptr)->Release(error);
  delete ptr;
  statement->private_data = nullptr;
  return status;
}

AdbcStatusCode HologresStatementSetOption(struct AdbcStatement* statement,
                                          const char* key, const char* value,
                                          struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode HologresStatementSetOptionBytes(struct AdbcStatement* statement,
                                               const char* key, const uint8_t* value,
                                               size_t length, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->SetOptionBytes(key, value, length, error);
}

AdbcStatusCode HologresStatementSetOptionDouble(struct AdbcStatement* statement,
                                                const char* key, double value,
                                                struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->SetOptionDouble(key, value, error);
}

AdbcStatusCode HologresStatementSetOptionInt(struct AdbcStatement* statement,
                                             const char* key, int64_t value,
                                             struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->SetOptionInt(key, value, error);
}

AdbcStatusCode HologresStatementSetSqlQuery(struct AdbcStatement* statement,
                                            const char* query, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<HologresStatement>*>(statement->private_data);
  return (*ptr)->SetSqlQuery(query, error);
}
}  // namespace

#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)
AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement,
                                 struct ArrowArray* values, struct ArrowSchema* schema,
                                 struct AdbcError* error) {
  return HologresStatementBind(statement, values, schema, error);
}

AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement* statement,
                                       struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  return HologresStatementBindStream(statement, stream, error);
}

AdbcStatusCode AdbcStatementCancel(struct AdbcStatement* statement,
                                   struct AdbcError* error) {
  return HologresStatementCancel(statement, error);
}

AdbcStatusCode AdbcStatementExecutePartitions(struct AdbcStatement* statement,
                                              ArrowSchema* schema,
                                              struct AdbcPartitions* partitions,
                                              int64_t* rows_affected,
                                              struct AdbcError* error) {
  return HologresStatementExecutePartitions(statement, schema, partitions, rows_affected,
                                            error);
}

AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* output,
                                         int64_t* rows_affected,
                                         struct AdbcError* error) {
  return HologresStatementExecuteQuery(statement, output, rows_affected, error);
}

AdbcStatusCode AdbcStatementExecuteSchema(struct AdbcStatement* statement,
                                          ArrowSchema* schema, struct AdbcError* error) {
  return HologresStatementExecuteSchema(statement, schema, error);
}

AdbcStatusCode AdbcStatementGetOption(struct AdbcStatement* statement, const char* key,
                                      char* value, size_t* length,
                                      struct AdbcError* error) {
  return HologresStatementGetOption(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementGetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, uint8_t* value,
                                           size_t* length, struct AdbcError* error) {
  return HologresStatementGetOptionBytes(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementGetOptionInt(struct AdbcStatement* statement, const char* key,
                                         int64_t* value, struct AdbcError* error) {
  return HologresStatementGetOptionInt(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double* value,
                                            struct AdbcError* error) {
  return HologresStatementGetOptionDouble(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetParameterSchema(struct AdbcStatement* statement,
                                               struct ArrowSchema* schema,
                                               struct AdbcError* error) {
  return HologresStatementGetParameterSchema(statement, schema, error);
}

AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement,
                                struct AdbcError* error) {
  return HologresStatementNew(connection, statement, error);
}

AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return HologresStatementPrepare(statement, error);
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return HologresStatementRelease(statement, error);
}

AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement* statement, const char* key,
                                      const char* value, struct AdbcError* error) {
  return HologresStatementSetOption(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, const uint8_t* value,
                                           size_t length, struct AdbcError* error) {
  return HologresStatementSetOptionBytes(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementSetOptionInt(struct AdbcStatement* statement, const char* key,
                                         int64_t value, struct AdbcError* error) {
  return HologresStatementSetOptionInt(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double value,
                                            struct AdbcError* error) {
  return HologresStatementSetOptionDouble(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error) {
  return HologresStatementSetSqlQuery(statement, query, error);
}
#endif  // ADBC_NO_COMMON_ENTRYPOINTS

// ---------------------------------------------------------------------
// AdbcDriverHologresInit

extern "C" {
ADBC_EXPORT
AdbcStatusCode AdbcDriverHologresInit(int version, void* raw_driver,
                                      struct AdbcError* error) {
  if (version != ADBC_VERSION_1_0_0 && version != ADBC_VERSION_1_1_0) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  if (!raw_driver) return ADBC_STATUS_INVALID_ARGUMENT;

  auto* driver = reinterpret_cast<struct AdbcDriver*>(raw_driver);
  if (version >= ADBC_VERSION_1_1_0) {
    std::memset(driver, 0, ADBC_DRIVER_1_1_0_SIZE);

    driver->ErrorGetDetailCount = HologresErrorGetDetailCount;
    driver->ErrorGetDetail = HologresErrorGetDetail;

    driver->DatabaseGetOption = HologresDatabaseGetOption;
    driver->DatabaseGetOptionBytes = HologresDatabaseGetOptionBytes;
    driver->DatabaseGetOptionDouble = HologresDatabaseGetOptionDouble;
    driver->DatabaseGetOptionInt = HologresDatabaseGetOptionInt;
    driver->DatabaseSetOptionBytes = HologresDatabaseSetOptionBytes;
    driver->DatabaseSetOptionDouble = HologresDatabaseSetOptionDouble;
    driver->DatabaseSetOptionInt = HologresDatabaseSetOptionInt;

    driver->ConnectionCancel = HologresConnectionCancel;
    driver->ConnectionGetOption = HologresConnectionGetOption;
    driver->ConnectionGetOptionBytes = HologresConnectionGetOptionBytes;
    driver->ConnectionGetOptionDouble = HologresConnectionGetOptionDouble;
    driver->ConnectionGetOptionInt = HologresConnectionGetOptionInt;
    driver->ConnectionGetStatistics = HologresConnectionGetStatistics;
    driver->ConnectionGetStatisticNames = HologresConnectionGetStatisticNames;
    driver->ConnectionSetOptionBytes = HologresConnectionSetOptionBytes;
    driver->ConnectionSetOptionDouble = HologresConnectionSetOptionDouble;
    driver->ConnectionSetOptionInt = HologresConnectionSetOptionInt;

    driver->StatementCancel = HologresStatementCancel;
    driver->StatementExecuteSchema = HologresStatementExecuteSchema;
    driver->StatementGetOption = HologresStatementGetOption;
    driver->StatementGetOptionBytes = HologresStatementGetOptionBytes;
    driver->StatementGetOptionDouble = HologresStatementGetOptionDouble;
    driver->StatementGetOptionInt = HologresStatementGetOptionInt;
    driver->StatementSetOptionBytes = HologresStatementSetOptionBytes;
    driver->StatementSetOptionDouble = HologresStatementSetOptionDouble;
    driver->StatementSetOptionInt = HologresStatementSetOptionInt;
  } else {
    std::memset(driver, 0, ADBC_DRIVER_1_0_0_SIZE);
  }

  driver->DatabaseInit = HologresDatabaseInit;
  driver->DatabaseNew = HologresDatabaseNew;
  driver->DatabaseRelease = HologresDatabaseRelease;
  driver->DatabaseSetOption = HologresDatabaseSetOption;

  driver->ConnectionCommit = HologresConnectionCommit;
  driver->ConnectionGetInfo = HologresConnectionGetInfo;
  driver->ConnectionGetObjects = HologresConnectionGetObjects;
  driver->ConnectionGetTableSchema = HologresConnectionGetTableSchema;
  driver->ConnectionGetTableTypes = HologresConnectionGetTableTypes;
  driver->ConnectionInit = HologresConnectionInit;
  driver->ConnectionNew = HologresConnectionNew;
  driver->ConnectionReadPartition = HologresConnectionReadPartition;
  driver->ConnectionRelease = HologresConnectionRelease;
  driver->ConnectionRollback = HologresConnectionRollback;
  driver->ConnectionSetOption = HologresConnectionSetOption;

  driver->StatementBind = HologresStatementBind;
  driver->StatementBindStream = HologresStatementBindStream;
  driver->StatementExecutePartitions = HologresStatementExecutePartitions;
  driver->StatementExecuteQuery = HologresStatementExecuteQuery;
  driver->StatementGetParameterSchema = HologresStatementGetParameterSchema;
  driver->StatementNew = HologresStatementNew;
  driver->StatementPrepare = HologresStatementPrepare;
  driver->StatementRelease = HologresStatementRelease;
  driver->StatementSetOption = HologresStatementSetOption;
  driver->StatementSetSqlQuery = HologresStatementSetSqlQuery;

  return ADBC_STATUS_OK;
}

#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)
ADBC_EXPORT
AdbcStatusCode AdbcDriverInit(int version, void* raw_driver, struct AdbcError* error) {
  return AdbcDriverHologresInit(version, raw_driver, error);
}
#endif  // ADBC_NO_COMMON_ENTRYPOINTS
}
