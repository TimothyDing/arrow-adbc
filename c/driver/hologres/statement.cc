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

// Windows
#define NOMINMAX

#ifdef _WIN32
#include <winsock2.h>
#endif

#include "statement.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cerrno>
#include <cinttypes>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <libpq-fe.h>
#include <nanoarrow/nanoarrow.hpp>

#include "bind_stream.h"
#include "connection.h"
#include "database.h"
#include "driver/common/options.h"
#include "driver/common/utils.h"
#include "driver/framework/utility.h"
#include "error.h"
#include "postgres_type.h"
#include "postgres_util.h"
#include "result_helper.h"
#include "result_reader.h"
#include "stage_connection.h"
#include "stage_writer.h"

namespace adbchg {

// ---------------------------------------------------------------------------
// TupleReader
// ---------------------------------------------------------------------------

int TupleReader::GetSchema(struct ArrowSchema* out) {
  assert(copy_reader_ != nullptr);
  ArrowErrorInit(&na_error_);

  int na_res = copy_reader_->GetSchema(out);
  if (out->release == nullptr) {
    InternalAdbcSetError(&error_, "[hologres] Result set was already consumed or freed");
    status_ = ADBC_STATUS_INVALID_STATE;
    return InternalAdbcStatusCodeToErrno(status_);
  } else if (na_res != NANOARROW_OK) {
    InternalAdbcSetError(&error_, "[hologres] Error copying schema");
    status_ = ADBC_STATUS_INTERNAL;
  }

  return na_res;
}

int TupleReader::GetCopyData() {
  if (pgbuf_ != nullptr) {
    PQfreemem(pgbuf_);
    pgbuf_ = nullptr;
  }

  data_.size_bytes = 0;
  data_.data.as_char = nullptr;

  int get_copy_res = PQgetCopyData(conn_, &pgbuf_, /*async=*/0);
  if (get_copy_res == -2) {
    InternalAdbcSetError(&error_, "[hologres] PQgetCopyData() failed: %s",
                         PQerrorMessage(conn_));
    status_ = ADBC_STATUS_IO;
    return InternalAdbcStatusCodeToErrno(status_);
  }

  if (get_copy_res == -1) {
    PQclear(result_);
    result_ = PQgetResult(conn_);
    const ExecStatusType pq_status = PQresultStatus(result_);
    if (pq_status != PGRES_COMMAND_OK) {
      status_ = adbcpq::SetError(&error_, result_, "[hologres] Execution error [%s]: %s",
                                 PQresStatus(pq_status), PQresultErrorMessage(result_));
      return InternalAdbcStatusCodeToErrno(status_);
    } else {
      return ENODATA;
    }
  }

  data_.size_bytes = get_copy_res;
  data_.data.as_char = pgbuf_;
  return NANOARROW_OK;
}

int TupleReader::AppendRowAndFetchNext() {
  int na_res = copy_reader_->ReadRecord(&data_, &na_error_);
  if (na_res != NANOARROW_OK && na_res != ENODATA) {
    InternalAdbcSetError(&error_, "[hologres] ReadRecord failed at row %" PRId64 ": %s",
                         row_id_, na_error_.message);
    status_ = ADBC_STATUS_IO;
    return na_res;
  }

  row_id_++;

  NANOARROW_RETURN_NOT_OK(GetCopyData());
  if ((copy_reader_->array_size_approx_bytes() + data_.size_bytes) >=
      batch_size_hint_bytes_) {
    return EOVERFLOW;
  }

  return NANOARROW_OK;
}

int TupleReader::BuildOutput(struct ArrowArray* out) {
  if (copy_reader_->array_size_approx_bytes() == 0) {
    out->release = nullptr;
    return NANOARROW_OK;
  }

  int na_res = copy_reader_->GetArray(out, &na_error_);
  if (na_res != NANOARROW_OK) {
    InternalAdbcSetError(&error_, "[hologres] Failed to build result array: %s",
                         na_error_.message);
    status_ = ADBC_STATUS_INTERNAL;
    return na_res;
  }

  return NANOARROW_OK;
}

int TupleReader::GetNext(struct ArrowArray* out) {
  if (is_finished_) {
    out->release = nullptr;
    return 0;
  }

  int na_res;
  ArrowErrorInit(&na_error_);

  if (row_id_ == -1) {
    na_res = GetCopyData();
    if (na_res == ENODATA) {
      is_finished_ = true;
      out->release = nullptr;
      return 0;
    } else if (na_res != NANOARROW_OK) {
      return na_res;
    }

    na_res = copy_reader_->ReadHeader(&data_, &na_error_);
    if (na_res != NANOARROW_OK) {
      InternalAdbcSetError(&error_, "[hologres] ReadHeader() failed: %s",
                           na_error_.message);
      return na_res;
    }

    row_id_++;
  }

  do {
    na_res = AppendRowAndFetchNext();
    if (na_res == EOVERFLOW) {
      return BuildOutput(out);
    }
  } while (na_res == NANOARROW_OK);

  if (na_res != ENODATA) {
    return na_res;
  }

  is_finished_ = true;

  NANOARROW_RETURN_NOT_OK(BuildOutput(out));
  return NANOARROW_OK;
}

void TupleReader::Release() {
  if (error_.release) {
    error_.release(&error_);
  }
  error_ = ADBC_ERROR_INIT;
  status_ = ADBC_STATUS_OK;

  if (result_) {
    PQclear(result_);
    result_ = nullptr;
  }

  if (pgbuf_) {
    PQfreemem(pgbuf_);
    pgbuf_ = nullptr;
  }

  if (copy_reader_) {
    copy_reader_.reset();
  }

  is_finished_ = false;
  row_id_ = -1;
}

struct ExportedTupleReader {
  std::weak_ptr<TupleReader> self;
};

void TupleReader::ExportTo(struct ArrowArrayStream* stream) {
  stream->get_schema = &GetSchemaTrampoline;
  stream->get_next = &GetNextTrampoline;
  stream->get_last_error = &GetLastErrorTrampoline;
  stream->release = &ReleaseTrampoline;
  stream->private_data = new ExportedTupleReader{weak_from_this()};
}

const struct AdbcError* TupleReader::ErrorFromArrayStream(struct ArrowArrayStream* self,
                                                          AdbcStatusCode* status) {
  if (!self->private_data || self->release != &ReleaseTrampoline) {
    return nullptr;
  }

  auto* wrapper = static_cast<ExportedTupleReader*>(self->private_data);
  auto maybe_reader = wrapper->self.lock();
  if (maybe_reader) {
    if (status) {
      *status = maybe_reader->status_;
    }
    return &maybe_reader->error_;
  }
  return nullptr;
}

int TupleReader::GetSchemaTrampoline(struct ArrowArrayStream* self,
                                     struct ArrowSchema* out) {
  if (!self || !self->private_data) return EINVAL;

  auto* wrapper = static_cast<ExportedTupleReader*>(self->private_data);
  auto maybe_reader = wrapper->self.lock();
  if (maybe_reader) {
    return maybe_reader->GetSchema(out);
  }
  return EINVAL;
}

int TupleReader::GetNextTrampoline(struct ArrowArrayStream* self,
                                   struct ArrowArray* out) {
  if (!self || !self->private_data) return EINVAL;

  auto* wrapper = static_cast<ExportedTupleReader*>(self->private_data);
  auto maybe_reader = wrapper->self.lock();
  if (maybe_reader) {
    return maybe_reader->GetNext(out);
  }
  return EINVAL;
}

const char* TupleReader::GetLastErrorTrampoline(struct ArrowArrayStream* self) {
  if (!self || !self->private_data) return nullptr;
  constexpr std::string_view kReaderInvalidated =
      "[hologres] Reader invalidated (statement or reader was closed)";

  auto* wrapper = static_cast<ExportedTupleReader*>(self->private_data);
  auto maybe_reader = wrapper->self.lock();
  if (maybe_reader) {
    return maybe_reader->last_error();
  }
  return kReaderInvalidated.data();
}

void TupleReader::ReleaseTrampoline(struct ArrowArrayStream* self) {
  if (!self || !self->private_data) return;

  auto* wrapper = static_cast<ExportedTupleReader*>(self->private_data);
  auto maybe_reader = wrapper->self.lock();
  if (maybe_reader) {
    maybe_reader->Release();
  }
  delete wrapper;
  self->private_data = nullptr;
  self->release = nullptr;
}

// ---------------------------------------------------------------------------
// HologresStatement
// ---------------------------------------------------------------------------

AdbcStatusCode HologresStatement::New(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  if (!connection || !connection->private_data) {
    InternalAdbcSetError(error, "%s",
                         "[hologres] Must provide an initialized AdbcConnection");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  connection_ =
      *reinterpret_cast<std::shared_ptr<HologresConnection>*>(connection->private_data);
  type_resolver_ = connection_->type_resolver();
  ClearResult();
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::Bind(struct ArrowArray* values,
                                       struct ArrowSchema* schema,
                                       struct AdbcError* error) {
  if (!values || !values->release) {
    InternalAdbcSetError(error, "%s", "[hologres] Must provide non-NULL array");
    return ADBC_STATUS_INVALID_ARGUMENT;
  } else if (!schema || !schema->release) {
    InternalAdbcSetError(error, "%s", "[hologres] Must provide non-NULL schema");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  if (bind_.release) bind_.release(&bind_);
  adbc::driver::MakeArrayStream(schema, values, &bind_);
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::Bind(struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  if (!stream || !stream->release) {
    InternalAdbcSetError(error, "%s", "[hologres] Must provide non-NULL stream");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  if (bind_.release) bind_.release(&bind_);
  bind_ = *stream;
  std::memset(stream, 0, sizeof(*stream));
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::Cancel(struct AdbcError* error) {
  return connection_->Cancel(error);
}

AdbcStatusCode HologresStatement::CreateBulkTable(
    const std::string& current_schema, const struct ArrowSchema& source_schema,
    std::string* escaped_table, std::string* escaped_field_list,
    std::string* escaped_type_list, struct AdbcError* error) {
  PGconn* conn = connection_->conn();

  if (!ingest_.db_schema.empty() && ingest_.temporary) {
    InternalAdbcSetError(error, "[hologres] Cannot set both %s and %s",
                         ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
                         ADBC_INGEST_OPTION_TEMPORARY);
    return ADBC_STATUS_INVALID_STATE;
  }

  {
    if (!ingest_.db_schema.empty()) {
      char* escaped =
          PQescapeIdentifier(conn, ingest_.db_schema.c_str(), ingest_.db_schema.size());
      if (escaped == nullptr) {
        InternalAdbcSetError(
            error, "[hologres] Failed to escape target schema %s for ingestion: %s",
            ingest_.db_schema.c_str(), PQerrorMessage(conn));
        return ADBC_STATUS_INTERNAL;
      }
      *escaped_table += escaped;
      *escaped_table += " . ";
      PQfreemem(escaped);
    } else if (ingest_.temporary) {
      *escaped_table += "pg_temp . ";
    } else {
      char* escaped =
          PQescapeIdentifier(conn, current_schema.c_str(), current_schema.size());
      *escaped_table += escaped;
      *escaped_table += " . ";
      PQfreemem(escaped);
    }

    if (!ingest_.target.empty()) {
      char* escaped =
          PQescapeIdentifier(conn, ingest_.target.c_str(), ingest_.target.size());
      if (escaped == nullptr) {
        InternalAdbcSetError(
            error, "[hologres] Failed to escape target table %s for ingestion: %s",
            ingest_.target.c_str(), PQerrorMessage(conn));
        return ADBC_STATUS_INTERNAL;
      }
      *escaped_table += escaped;
      PQfreemem(escaped);
    }
  }

  std::string create;

  if (ingest_.temporary) {
    create = "CREATE TEMPORARY TABLE ";
  } else {
    create = "CREATE TABLE ";
  }

  switch (ingest_.mode) {
    case IngestMode::kCreate:
    case IngestMode::kAppend:
      break;
    case IngestMode::kReplace: {
      std::string drop = "DROP TABLE IF EXISTS " + *escaped_table;
      PGresult* result = PQexecParams(conn, drop.c_str(), /*nParams=*/0,
                                      /*paramTypes=*/nullptr, /*paramValues=*/nullptr,
                                      /*paramLengths=*/nullptr, /*paramFormats=*/nullptr,
                                      /*resultFormat=*/1 /*(binary)*/);
      if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        AdbcStatusCode code = adbcpq::SetError(
            error, result, "[hologres] Failed to drop table: %s\nQuery was: %s",
            PQerrorMessage(conn), drop.c_str());
        PQclear(result);
        return code;
      }
      PQclear(result);
      break;
    }
    case IngestMode::kCreateAppend:
      create += "IF NOT EXISTS ";
      break;
  }
  create += *escaped_table;
  create += " (";

  for (int64_t i = 0; i < source_schema.n_children; i++) {
    if (i > 0) {
      create += ", ";
      *escaped_field_list += ", ";
      if (escaped_type_list) *escaped_type_list += ", ";
    }

    const char* unescaped = source_schema.children[i]->name;
    char* escaped = PQescapeIdentifier(conn, unescaped, std::strlen(unescaped));
    if (escaped == nullptr) {
      InternalAdbcSetError(error,
                           "[hologres] Failed to escape column %s for ingestion: %s",
                           unescaped, PQerrorMessage(conn));
      return ADBC_STATUS_INTERNAL;
    }
    create += escaped;
    *escaped_field_list += escaped;
    PQfreemem(escaped);

    adbcpq::PostgresType pg_type;
    struct ArrowError na_error;
    CHECK_NA_DETAIL(
        INTERNAL,
        adbcpq::PostgresType::FromSchema(*type_resolver_, source_schema.children[i],
                                         &pg_type, &na_error),
        &na_error, error);
    std::string type_name = pg_type.sql_type_name();
    create += " " + type_name;

    // Build type list for EXTERNAL_FILES AS clause: "col_name" type
    if (escaped_type_list) {
      char* escaped2 = PQescapeIdentifier(conn, unescaped, std::strlen(unescaped));
      *escaped_type_list += escaped2;
      PQfreemem(escaped2);
      *escaped_type_list += " " + type_name;
    }
  }

  if (ingest_.mode == IngestMode::kAppend) {
    return ADBC_STATUS_OK;
  }

  create += ")";
  PGresult* result = PQexecParams(conn, create.c_str(), /*nParams=*/0,
                                  /*paramTypes=*/nullptr, /*paramValues=*/nullptr,
                                  /*paramLengths=*/nullptr, /*paramFormats=*/nullptr,
                                  /*resultFormat=*/1 /*(binary)*/);
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    AdbcStatusCode code = adbcpq::SetError(
        error, result, "[hologres] Failed to create table: %s\nQuery was: %s",
        PQerrorMessage(conn), create.c_str());
    PQclear(result);
    return code;
  }
  PQclear(result);
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::ExecuteBind(struct ArrowArrayStream* stream,
                                              int64_t* rows_affected,
                                              struct AdbcError* error) {
  adbcpq::PqResultArrayReader reader(connection_->conn(), type_resolver_, query_);
  reader.SetAutocommit(connection_->autocommit());
  reader.SetBind(&bind_);
  reader.SetVendorName("Hologres");
  RAISE_STATUS(error, reader.ToArrayStream(rows_affected, stream));
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::ExecuteQuery(struct ArrowArrayStream* stream,
                                               int64_t* rows_affected,
                                               struct AdbcError* error) {
  ClearResult();

  // Use a dedicated path to handle bulk ingest
  if (!ingest_.target.empty()) {
    return ExecuteIngest(stream, rows_affected, error);
  }

  if (query_.empty()) {
    InternalAdbcSetError(error, "%s",
                         "[hologres] Must SetSqlQuery before ExecuteQuery");
    return ADBC_STATUS_INVALID_STATE;
  }

  // Use a dedicated path to handle parameter binding
  if (bind_.release != nullptr) {
    return ExecuteBind(stream, rows_affected, error);
  }

  // If no output requested or COPY disabled, use PqResultArrayReader
  if (!stream || !use_copy_) {
    adbcpq::PqResultArrayReader reader(connection_->conn(), type_resolver_, query_);
    reader.SetVendorName("Hologres");
    RAISE_STATUS(error, reader.ToArrayStream(rows_affected, stream));
    return ADBC_STATUS_OK;
  }

  adbcpq::PqResultHelper helper(connection_->conn(), query_);
  RAISE_STATUS(error, helper.Prepare());
  RAISE_STATUS(error, helper.DescribePrepared());

  adbcpq::PostgresType root_type;
  RAISE_STATUS(error, helper.ResolveOutputTypes(*type_resolver_, &root_type));

  // If there will be no columns in the result, avoid COPY
  if (root_type.n_children() == 0) {
    adbcpq::PqResultArrayReader reader(connection_->conn(), type_resolver_, query_);
    reader.SetVendorName("Hologres");
    RAISE_STATUS(error, reader.ToArrayStream(rows_affected, stream));
    return ADBC_STATUS_OK;
  }

  struct ArrowError na_error;
  reader_->copy_reader_ = std::make_unique<adbcpq::PostgresCopyStreamReader>();
  CHECK_NA(INTERNAL, reader_->copy_reader_->Init(root_type), error);
  CHECK_NA_DETAIL(INTERNAL,
                  reader_->copy_reader_->InferOutputSchema("Hologres", &na_error),
                  &na_error, error);

  CHECK_NA_DETAIL(INTERNAL, reader_->copy_reader_->InitFieldReaders(&na_error), &na_error,
                  error);

  // Execute the COPY query
  RAISE_STATUS(error, helper.ExecuteCopy());

  // We need the PQresult back for the reader
  reader_->result_ = helper.ReleaseResult();

  // Export to stream
  reader_->ExportTo(stream);
  if (rows_affected) *rows_affected = -1;
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::ExecuteSchema(struct ArrowSchema* schema,
                                                struct AdbcError* error) {
  ClearResult();
  if (query_.empty()) {
    InternalAdbcSetError(error, "%s",
                         "[hologres] Must SetSqlQuery before ExecuteSchema");
    return ADBC_STATUS_INVALID_STATE;
  }

  adbcpq::PqResultHelper helper(connection_->conn(), query_);

  if (bind_.release) {
    nanoarrow::UniqueSchema param_schema;
    struct ArrowError na_error;
    ArrowErrorInit(&na_error);
    CHECK_NA_DETAIL(INTERNAL,
                    ArrowArrayStreamGetSchema(&bind_, param_schema.get(), &na_error),
                    &na_error, error);

    if (std::string(param_schema->format) != "+s") {
      InternalAdbcSetError(error, "%s",
                           "[hologres] Bind parameters must have type STRUCT");
      return ADBC_STATUS_INVALID_STATE;
    }

    std::vector<Oid> param_oids(param_schema->n_children);
    for (int64_t i = 0; i < param_schema->n_children; i++) {
      adbcpq::PostgresType pg_type;
      CHECK_NA_DETAIL(
          INTERNAL,
          adbcpq::PostgresType::FromSchema(*type_resolver_, param_schema->children[i],
                                           &pg_type, &na_error),
          &na_error, error);
      param_oids[i] = pg_type.oid();
    }

    RAISE_STATUS(error, helper.Prepare(param_oids));
  } else {
    RAISE_STATUS(error, helper.Prepare());
  }

  RAISE_STATUS(error, helper.DescribePrepared());

  adbcpq::PostgresType output_type;
  RAISE_STATUS(error, helper.ResolveOutputTypes(*type_resolver_, &output_type));

  nanoarrow::UniqueSchema tmp;
  ArrowSchemaInit(tmp.get());
  CHECK_NA(INTERNAL, output_type.SetSchema(tmp.get(), "Hologres"), error);

  tmp.move(schema);
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::ExecuteIngest(struct ArrowArrayStream* stream,
                                                int64_t* rows_affected,
                                                struct AdbcError* error) {
  if (!bind_.release) {
    InternalAdbcSetError(error, "%s",
                         "[hologres] Must Bind() before Execute() for bulk ingestion");
    return ADBC_STATUS_INVALID_STATE;
  }

  if (stream != nullptr) {
    InternalAdbcSetError(error, "%s",
                         "[hologres] Bulk ingest with result set is not supported");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  // Dispatch to Stage mode if configured
  if (ingest_.hologres_ingest_method == HologresIngestMethod::kStage) {
    return ExecuteIngestStage(stream, rows_affected, error);
  }

  // Get current schema to avoid temp table shadowing
  std::string current_schema;
  {
    adbcpq::PqResultHelper result_helper{connection_->conn(), "SELECT CURRENT_SCHEMA()"};
    RAISE_STATUS(error, result_helper.Execute());
    auto it = result_helper.begin();
    if (it == result_helper.end()) {
      InternalAdbcSetError(
          error,
          "[hologres] PostgreSQL returned no rows for 'SELECT CURRENT_SCHEMA()'");
      return ADBC_STATUS_INTERNAL;
    }
    current_schema = (*it)[0].data;
  }

  adbcpq::BindStream bind_stream;
  bind_stream.SetBind(&bind_);
  std::memset(&bind_, 0, sizeof(bind_));
  std::string escaped_table;
  std::string escaped_field_list;
  RAISE_STATUS(error, bind_stream.Begin([&]() -> adbc::driver::Status {
    struct AdbcError tmp_error = ADBC_ERROR_INIT;
    AdbcStatusCode status_code = CreateBulkTable(current_schema,
                                                 bind_stream.bind_schema.value,
                                                 &escaped_table, &escaped_field_list,
                                                 /*escaped_type_list=*/nullptr,
                                                 &tmp_error);
    return adbc::driver::Status::FromAdbc(status_code, tmp_error);
  }));

  // Build COPY query with Hologres extensions
  std::string query = "COPY ";
  query += escaped_table;
  query += " (";
  query += escaped_field_list;
  query += ") FROM STDIN WITH (FORMAT binary";

  // Add Hologres-specific STREAM_MODE
  query += ", STREAM_MODE TRUE";

  // Add ON_CONFLICT clause if configured
  switch (ingest_.on_conflict) {
    case OnConflictMode::kNone:
      break;
    case OnConflictMode::kIgnore:
      query += ", ON_CONFLICT 'ignore'";
      break;
    case OnConflictMode::kUpdate:
      query += ", ON_CONFLICT 'update'";
      break;
  }

  query += ")";

  PGresult* result = PQexec(connection_->conn(), query.c_str());
  if (PQresultStatus(result) != PGRES_COPY_IN) {
    AdbcStatusCode code = adbcpq::SetError(
        error, result, "[hologres] COPY query failed: %s\nQuery was: %s",
        PQerrorMessage(connection_->conn()), query.c_str());
    PQclear(result);
    return code;
  }

  PQclear(result);
  RAISE_STATUS(error, bind_stream.ExecuteCopy(connection_->conn(),
                                              *connection_->type_resolver(),
                                              rows_affected));
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::ExecuteIngestStage(struct ArrowArrayStream* stream,
                                                     int64_t* rows_affected,
                                                     struct AdbcError* error) {
  // Get current schema
  std::string current_schema;
  {
    adbcpq::PqResultHelper result_helper{connection_->conn(), "SELECT CURRENT_SCHEMA()"};
    RAISE_STATUS(error, result_helper.Execute());
    auto it = result_helper.begin();
    if (it == result_helper.end()) {
      InternalAdbcSetError(
          error,
          "[hologres] PostgreSQL returned no rows for 'SELECT CURRENT_SCHEMA()'");
      return ADBC_STATUS_INTERNAL;
    }
    current_schema = (*it)[0].data;
  }

  // Setup bind stream and create table
  adbcpq::BindStream bind_stream;
  bind_stream.SetBind(&bind_);
  std::memset(&bind_, 0, sizeof(bind_));
  std::string escaped_table;
  std::string escaped_field_list;
  std::string escaped_type_list;
  RAISE_STATUS(error, bind_stream.Begin([&]() -> adbc::driver::Status {
    struct AdbcError tmp_error = ADBC_ERROR_INIT;
    AdbcStatusCode status_code =
        CreateBulkTable(current_schema, bind_stream.bind_schema.value, &escaped_table,
                        &escaped_field_list, &escaped_type_list, &tmp_error);
    return adbc::driver::Status::FromAdbc(status_code, tmp_error);
  }));

  // Create stage configuration
  HologresStageConfig config;
  config.stage_name = HologresStageWriter::GenerateStageName();

  // Create a dedicated FixedFE connection for COPY operations only.
  // Hologres COPY EXTERNAL_FILES requires FixedFE connection (options=type=fixed)
  // but CALL statements (HG_CREATE/DROP_INTERNAL_STAGE) require regular FE.
  std::string fixed_uri = MakeFixedFeUri(connection_->database()->uri());
  PGconn* fixed_conn = PQconnectdb(fixed_uri.c_str());
  if (PQstatus(fixed_conn) != CONNECTION_OK) {
    std::string conn_error = PQerrorMessage(fixed_conn);
    PQfinish(fixed_conn);
    InternalAdbcSetError(
        error, "[hologres] Failed to create FixedFE connection for Stage: %s",
        conn_error.c_str());
    return ADBC_STATUS_IO;
  }

  // Ensure FixedFE connection is cleaned up on scope exit
  auto fixed_conn_guard =
      std::unique_ptr<PGconn, decltype(&PQfinish)>(fixed_conn, PQfinish);

  // Create stage using main connection (CALL statements need regular FE)
  auto main_stage_conn = StageConnection::CreateFromPGconn(connection_->conn());
  HologresStageWriter main_writer(main_stage_conn.get(), config);
  RAISE_STATUS(error, main_writer.CreateStage());

  // Write to stage using FixedFE connection (COPY EXTERNAL_FILES needs FixedFE)
  {
    auto fixed_stage_conn = StageConnection::CreateFromPGconn(fixed_conn);
    HologresStageWriter fixed_writer(fixed_stage_conn.get(), config);
    RAISE_STATUS(error, fixed_writer.WriteToStage(&bind_stream.bind.value, rows_affected));
  }

  // Query primary key columns if ON_CONFLICT UPDATE is requested
  std::string pk_columns;
  if (ingest_.on_conflict == OnConflictMode::kUpdate) {
    // Use schema-qualified lookup with pg_class + pg_namespace
    std::string pk_query = fmt::format(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid "
        "AND a.attnum = ANY(i.indkey) "
        "JOIN pg_class c ON c.oid = i.indrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = '{}' AND c.relname = '{}' AND i.indisprimary "
        "ORDER BY array_position(i.indkey, a.attnum)",
        current_schema, ingest_.target);
    PGresult* pk_result = PQexec(connection_->conn(), pk_query.c_str());
    if (PQresultStatus(pk_result) == PGRES_TUPLES_OK) {
      for (int r = 0; r < PQntuples(pk_result); r++) {
        if (r > 0) pk_columns += ", ";
        char* escaped = PQescapeIdentifier(connection_->conn(),
                                           PQgetvalue(pk_result, r, 0),
                                           strlen(PQgetvalue(pk_result, r, 0)));
        pk_columns += escaped;
        PQfreemem(escaped);
      }
    }
    PQclear(pk_result);
    if (pk_columns.empty()) {
      InternalAdbcSetError(
          error,
          "[hologres] ON_CONFLICT UPDATE in Stage mode requires a primary key");
      main_writer.DropStage();
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
  }

  // Insert from stage using main connection
  RAISE_STATUS(error, main_writer.InsertFromStage(escaped_table, escaped_field_list,
                                                   escaped_type_list, ingest_.on_conflict,
                                                   pk_columns));
  RAISE_STATUS(error, main_writer.DropStage());

  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::GetOption(const char* key, char* value, size_t* length,
                                            struct AdbcError* error) {
  std::string result;
  if (std::strcmp(key, ADBC_INGEST_OPTION_TARGET_TABLE) == 0) {
    result = ingest_.target;
  } else if (std::strcmp(key, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA) == 0) {
    result = ingest_.db_schema;
  } else if (std::strcmp(key, ADBC_INGEST_OPTION_MODE) == 0) {
    switch (ingest_.mode) {
      case IngestMode::kCreate:
        result = ADBC_INGEST_OPTION_MODE_CREATE;
        break;
      case IngestMode::kAppend:
        result = ADBC_INGEST_OPTION_MODE_APPEND;
        break;
      case IngestMode::kReplace:
        result = ADBC_INGEST_OPTION_MODE_REPLACE;
        break;
      case IngestMode::kCreateAppend:
        result = ADBC_INGEST_OPTION_MODE_CREATE_APPEND;
        break;
    }
  } else if (std::strcmp(key, ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES) == 0) {
    result = std::to_string(reader_->batch_size_hint_bytes_);
  } else if (std::strcmp(key, ADBC_HOLOGRES_OPTION_USE_COPY) == 0) {
    result = use_copy_ ? "true" : "false";
  } else if (std::strcmp(key, ADBC_HOLOGRES_OPTION_ON_CONFLICT) == 0) {
    switch (ingest_.on_conflict) {
      case OnConflictMode::kNone:
        result = "none";
        break;
      case OnConflictMode::kIgnore:
        result = "ignore";
        break;
      case OnConflictMode::kUpdate:
        result = "update";
        break;
    }
  } else if (std::strcmp(key, ADBC_HOLOGRES_OPTION_INGEST_MODE) == 0) {
    switch (ingest_.hologres_ingest_method) {
      case HologresIngestMethod::kCopy:
        result = "copy";
        break;
      case HologresIngestMethod::kStage:
        result = "stage";
        break;
    }
  } else {
    InternalAdbcSetError(error, "[hologres] Unknown statement option '%s'", key);
    return ADBC_STATUS_NOT_FOUND;
  }

  if (result.size() + 1 <= *length) {
    std::memcpy(value, result.data(), result.size() + 1);
  }
  *length = static_cast<int64_t>(result.size() + 1);
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::GetOptionBytes(const char* key, uint8_t* value,
                                                 size_t* length,
                                                 struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown statement option '%s'", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresStatement::GetOptionDouble(const char* key, double* value,
                                                  struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown statement option '%s'", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresStatement::GetOptionInt(const char* key, int64_t* value,
                                               struct AdbcError* error) {
  if (std::strcmp(key, ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES) == 0) {
    *value = reader_->batch_size_hint_bytes_;
    return ADBC_STATUS_OK;
  }
  InternalAdbcSetError(error, "[hologres] Unknown statement option '%s'", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresStatement::GetParameterSchema(struct ArrowSchema* schema,
                                                     struct AdbcError* error) {
  if (query_.empty()) {
    InternalAdbcSetError(error,
                         "[hologres] Must SetSqlQuery before GetParameterSchema");
    return ADBC_STATUS_INVALID_STATE;
  }

  adbcpq::PqResultHelper helper(connection_->conn(), query_);
  RAISE_STATUS(error, helper.Prepare());
  RAISE_STATUS(error, helper.DescribePrepared());
  adbcpq::PostgresType param_types;
  RAISE_STATUS(error,
               helper.ResolveParamTypes(*connection_->type_resolver(), &param_types));

  ArrowSchemaInit(schema);

  RAISE_NA(param_types.SetSchema(schema, "Hologres"));

  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::Prepare(struct AdbcError* error) {
  if (query_.empty()) {
    InternalAdbcSetError(error, "%s",
                         "[hologres] Must SetSqlQuery() before Prepare()");
    return ADBC_STATUS_INVALID_STATE;
  }

  prepared_ = true;
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::Release(struct AdbcError* error) {
  ClearResult();
  if (bind_.release) {
    bind_.release(&bind_);
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::SetSqlQuery(const char* query,
                                              struct AdbcError* error) {
  ingest_.target.clear();
  ingest_.db_schema.clear();
  query_ = query;
  prepared_ = false;
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::SetOption(const char* key, const char* value,
                                            struct AdbcError* error) {
  if (std::strcmp(key, ADBC_INGEST_OPTION_TARGET_TABLE) == 0) {
    query_.clear();
    ingest_.target = value;
    prepared_ = false;
  } else if (std::strcmp(key, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA) == 0) {
    query_.clear();
    if (value == nullptr) {
      ingest_.db_schema.clear();
    } else {
      ingest_.db_schema = value;
    }
    prepared_ = false;
  } else if (std::strcmp(key, ADBC_INGEST_OPTION_MODE) == 0) {
    if (std::strcmp(value, ADBC_INGEST_OPTION_MODE_CREATE) == 0) {
      ingest_.mode = IngestMode::kCreate;
    } else if (std::strcmp(value, ADBC_INGEST_OPTION_MODE_APPEND) == 0) {
      ingest_.mode = IngestMode::kAppend;
    } else if (std::strcmp(value, ADBC_INGEST_OPTION_MODE_REPLACE) == 0) {
      ingest_.mode = IngestMode::kReplace;
    } else if (std::strcmp(value, ADBC_INGEST_OPTION_MODE_CREATE_APPEND) == 0) {
      ingest_.mode = IngestMode::kCreateAppend;
    } else {
      InternalAdbcSetError(error, "[hologres] Invalid value '%s' for option '%s'", value,
                           key);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    prepared_ = false;
  } else if (std::strcmp(key, ADBC_INGEST_OPTION_TEMPORARY) == 0) {
    if (std::strcmp(value, ADBC_OPTION_VALUE_ENABLED) == 0) {
      ingest_.temporary = true;
      ingest_.db_schema.clear();
    } else if (std::strcmp(value, ADBC_OPTION_VALUE_DISABLED) == 0) {
      ingest_.temporary = false;
    } else {
      InternalAdbcSetError(error, "[hologres] Invalid value '%s' for option '%s'", value,
                           key);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    prepared_ = false;
  } else if (std::strcmp(key, ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES) == 0) {
    int64_t int_value = std::atol(value);
    if (int_value <= 0) {
      InternalAdbcSetError(error, "[hologres] Invalid value '%s' for option '%s'", value,
                           key);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    this->batch_size_hint_bytes_ = this->reader_->batch_size_hint_bytes_ = int_value;
  } else if (std::strcmp(key, ADBC_HOLOGRES_OPTION_USE_COPY) == 0) {
    if (std::strcmp(value, ADBC_OPTION_VALUE_ENABLED) == 0) {
      use_copy_ = true;
    } else if (std::strcmp(value, ADBC_OPTION_VALUE_DISABLED) == 0) {
      use_copy_ = false;
    } else {
      InternalAdbcSetError(error, "[hologres] Invalid value '%s' for option '%s'", value,
                           key);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
  } else if (std::strcmp(key, ADBC_HOLOGRES_OPTION_ON_CONFLICT) == 0) {
    if (std::strcmp(value, "none") == 0) {
      ingest_.on_conflict = OnConflictMode::kNone;
    } else if (std::strcmp(value, "ignore") == 0) {
      ingest_.on_conflict = OnConflictMode::kIgnore;
    } else if (std::strcmp(value, "update") == 0) {
      ingest_.on_conflict = OnConflictMode::kUpdate;
    } else {
      InternalAdbcSetError(error, "[hologres] Invalid value '%s' for option '%s'", value,
                           key);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
  } else if (std::strcmp(key, ADBC_HOLOGRES_OPTION_INGEST_MODE) == 0) {
    if (std::strcmp(value, "copy") == 0) {
      ingest_.hologres_ingest_method = HologresIngestMethod::kCopy;
    } else if (std::strcmp(value, "stage") == 0) {
      ingest_.hologres_ingest_method = HologresIngestMethod::kStage;
    } else {
      InternalAdbcSetError(error, "[hologres] Invalid value '%s' for option '%s'", value,
                           key);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
  } else {
    InternalAdbcSetError(error, "[hologres] Unknown statement option '%s'", key);
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresStatement::SetOptionBytes(const char* key, const uint8_t* value,
                                                 size_t length,
                                                 struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown statement option '%s'", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::SetOptionDouble(const char* key, double value,
                                                  struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown statement option '%s'", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresStatement::SetOptionInt(const char* key, int64_t value,
                                               struct AdbcError* error) {
  if (std::strcmp(key, ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES) == 0) {
    if (value <= 0) {
      InternalAdbcSetError(error,
                           "[hologres] Invalid value '%" PRIi64 "' for option '%s'",
                           value, key);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    this->batch_size_hint_bytes_ = this->reader_->batch_size_hint_bytes_ = value;
    return ADBC_STATUS_OK;
  }
  InternalAdbcSetError(error, "[hologres] Unknown statement option '%s'", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

void HologresStatement::ClearResult() {
  if (reader_) reader_->Release();
  reader_ = std::make_shared<TupleReader>(connection_->conn());
  reader_->batch_size_hint_bytes_ = batch_size_hint_bytes_;
}

}  // namespace adbchg
