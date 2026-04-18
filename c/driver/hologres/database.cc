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

#include "database.h"

#include <array>
#include <charconv>
#include <cinttypes>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <libpq-fe.h>
#include <nanoarrow/nanoarrow.h>

#include "driver/common/utils.h"
#include "result_helper.h"

namespace adbchg {

HologresDatabase::HologresDatabase() : open_connections_(0) {
  type_resolver_ = std::make_shared<adbcpq::PostgresTypeResolver>();
}
HologresDatabase::~HologresDatabase() = default;

AdbcStatusCode HologresDatabase::GetOption(const char* option, char* value,
                                           size_t* length, struct AdbcError* error) {
  if (std::strcmp(option, ADBC_OPTION_URI) == 0) {
    size_t required = uri_.size() + 1;
    if (*length >= required) {
      std::memcpy(value, uri_.c_str(), required);
    }
    *length = required;
    return ADBC_STATUS_OK;
  }
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", option);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresDatabase::GetOptionBytes(const char* option, uint8_t* value,
                                                size_t* length, struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", option);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresDatabase::GetOptionInt(const char* option, int64_t* value,
                                              struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", option);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresDatabase::GetOptionDouble(const char* option, double* value,
                                                 struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", option);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresDatabase::Init(struct AdbcError* error) {
  if (uri_.empty()) {
    InternalAdbcSetError(error, "%s",
                         "[hologres] Must set URI before initializing database");
    return ADBC_STATUS_INVALID_STATE;
  }

  PGconn* conn = nullptr;
  RAISE_ADBC(Connect(&conn, error));

  Status status = InitVersions(conn);
  if (!status.ok()) {
    RAISE_ADBC(Disconnect(&conn, nullptr));
    return status.ToAdbc(error);
  }

  status = RebuildTypeResolver(conn);
  RAISE_ADBC(Disconnect(&conn, nullptr));
  return status.ToAdbc(error);
}

AdbcStatusCode HologresDatabase::Release(struct AdbcError* error) {
  if (open_connections_ != 0) {
    InternalAdbcSetError(error, "%s%" PRId32 "%s",
                         "[hologres] Database released with ", open_connections_,
                         " open connections");
    return ADBC_STATUS_INVALID_STATE;
  }
  uri_.clear();
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresDatabase::SetOption(const char* key, const char* value,
                                           struct AdbcError* error) {
  if (std::strcmp(key, ADBC_OPTION_URI) == 0) {
    uri_ = value;
    return ADBC_STATUS_OK;
  }
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresDatabase::SetOptionBytes(const char* key, const uint8_t* value,
                                                size_t length, struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresDatabase::SetOptionDouble(const char* key, double value,
                                                 struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresDatabase::SetOptionInt(const char* key, int64_t value,
                                              struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresDatabase::Connect(PGconn** conn, struct AdbcError* error) {
  if (uri_.empty()) {
    InternalAdbcSetError(
        error, "%s",
        "[hologres] Must set database option 'uri' before creating a connection");
    return ADBC_STATUS_INVALID_STATE;
  }
  *conn = PQconnectdb(uri_.c_str());
  if (PQstatus(*conn) != CONNECTION_OK) {
    InternalAdbcSetError(error, "%s%s",
                         "[hologres] Failed to connect: ", PQerrorMessage(*conn));
    PQfinish(*conn);
    *conn = nullptr;
    return ADBC_STATUS_IO;
  }
  open_connections_++;
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresDatabase::Disconnect(PGconn** conn, struct AdbcError* error) {
  PQfinish(*conn);
  *conn = nullptr;
  if (--open_connections_ < 0) {
    InternalAdbcSetError(error, "%s", "[hologres] Open connection count underflowed");
    return ADBC_STATUS_INTERNAL;
  }
  return ADBC_STATUS_OK;
}

namespace {

std::array<int, 3> ParseVersion(std::string_view version) {
  std::array<int, 3> out{};
  size_t component = 0;
  size_t component_begin = 0;
  size_t component_end = 0;

  while (component_begin < version.size() && component < out.size()) {
    component_end = version.find_first_of(".-", component_begin);
    if (component_end == version.npos) {
      component_end = version.size();
    }

    int value = 0;
    std::from_chars(version.data() + component_begin, version.data() + component_end,
                    value);
    out[component] = value;

    component_begin = component_end + 1;
    component_end = component_begin;
    component++;
  }

  return out;
}

}  // namespace

std::array<int, 3> ParsePrefixedVersion(std::string_view version_info,
                                        std::string_view prefix) {
  size_t pos = version_info.find(prefix);
  if (pos == version_info.npos) {
    return {0, 0, 0};
  }

  pos = version_info.find_first_not_of(' ', pos + prefix.size());
  if (pos == version_info.npos) {
    return {0, 0, 0};
  }

  return ParseVersion(version_info.substr(pos));
}

std::string MakeFixedFeUri(const std::string& uri) {
  const char* const kFixedOption = "options=type%3Dfixed";

  size_t query_pos = uri.find('?');

  if (query_pos == std::string::npos) {
    return uri + "?" + kFixedOption;
  }

  size_t options_pos = uri.find("options=", query_pos);
  if (options_pos == std::string::npos) {
    return uri + "&" + kFixedOption;
  }

  // Prepend type=fixed to existing options
  std::string result = uri;
  result.insert(options_pos + 8, "type%3Dfixed%20");
  return result;
}

Status HologresDatabase::InitVersions(PGconn* conn) {
  // Try Hologres-specific version function
  adbcpq::PqResultHelper hg_helper(conn, "SELECT hg_version();");
  Status hg_status = hg_helper.Execute();

  if (hg_status.ok() && hg_helper.NumRows() == 1 && hg_helper.NumColumns() == 1) {
    std::string_view hg_version_info = hg_helper.Row(0)[0].value();
    if (!hg_version_info.empty()) {
      // Parse from: "Hologres 4.1.12 (tag: ...), compatible with PostgreSQL 11.3 ..."
      hologres_server_version_ = ParsePrefixedVersion(hg_version_info, "Hologres");
      if (hologres_server_version_[0] != 0) {
        postgres_server_version_ = ParsePrefixedVersion(hg_version_info, "PostgreSQL");
        return Status::Ok();
      }
    }
  }

  // Fall back to standard PostgreSQL version detection
  adbcpq::PqResultHelper helper(conn, "SELECT version();");
  UNWRAP_STATUS(helper.Execute());
  if (helper.NumRows() != 1 || helper.NumColumns() != 1) {
    return Status::Internal("Expected 1 row and 1 column for SELECT version(); but got ",
                            helper.NumRows(), "/", helper.NumColumns());
  }

  std::string_view version_info = helper.Row(0)[0].value();
  postgres_server_version_ = ParsePrefixedVersion(version_info, "PostgreSQL");

  return Status::Ok();
}

static std::string BuildPgTypeQuery();

static Status InsertPgAttributeResult(
    const adbcpq::PqResultHelper& result,
    const std::shared_ptr<adbcpq::PostgresTypeResolver>& resolver);

static Status InsertPgTypeResult(
    const adbcpq::PqResultHelper& result,
    const std::shared_ptr<adbcpq::PostgresTypeResolver>& resolver);

Status HologresDatabase::RebuildTypeResolver(PGconn* conn) {
  const std::string kColumnsQuery = R"(
SELECT
    attrelid,
    attname,
    atttypid
FROM
    pg_catalog.pg_attribute
ORDER BY
    attrelid, attnum
)";

  // Hologres always has typarray (PostgreSQL-compatible)
  std::string type_query = BuildPgTypeQuery();

  auto resolver = std::make_shared<adbcpq::PostgresTypeResolver>();

  adbcpq::PqResultHelper columns(conn, kColumnsQuery.c_str());
  UNWRAP_STATUS(columns.Execute());
  UNWRAP_STATUS(InsertPgAttributeResult(columns, resolver));

  int32_t max_attempts = 3;
  adbcpq::PqResultHelper types(conn, type_query);
  for (int32_t i = 0; i < max_attempts; i++) {
    UNWRAP_STATUS(types.Execute());
    UNWRAP_STATUS(InsertPgTypeResult(types, resolver));
  }

  type_resolver_ = std::move(resolver);
  return Status::Ok();
}

static std::string BuildPgTypeQuery() {
  // Hologres always has typarray column
  return "SELECT oid, typname, typreceive, typbasetype, typrelid, typarray"
         " FROM pg_catalog.pg_type"
         " WHERE (typreceive != 0 OR typsend != 0) AND typtype != 'r'"
         " AND typreceive::TEXT != 'array_recv'";
}

static Status InsertPgAttributeResult(
    const adbcpq::PqResultHelper& result,
    const std::shared_ptr<adbcpq::PostgresTypeResolver>& resolver) {
  int num_rows = result.NumRows();
  std::vector<std::pair<std::string, uint32_t>> columns;
  int64_t current_type_oid = 0;

  if (result.NumColumns() != 3) {
    return Status::Internal(
        "Expected 3 columns from type resolver pg_attribute query but got ",
        result.NumColumns());
  }

  for (int row = 0; row < num_rows; row++) {
    adbcpq::PqResultRow item = result.Row(row);
    int64_t type_oid;
    UNWRAP_RESULT(type_oid, item[0].ParseInteger());
    std::string_view col_name = item[1].value();
    int64_t col_oid;
    UNWRAP_RESULT(col_oid, item[2].ParseInteger());

    if (type_oid != current_type_oid && !columns.empty()) {
      resolver->InsertClass(static_cast<uint32_t>(current_type_oid), columns);
      columns.clear();
      current_type_oid = type_oid;
    }

    columns.push_back({std::string(col_name), static_cast<uint32_t>(col_oid)});
  }

  if (!columns.empty()) {
    resolver->InsertClass(static_cast<uint32_t>(current_type_oid), columns);
  }

  return Status::Ok();
}

static Status InsertPgTypeResult(
    const adbcpq::PqResultHelper& result,
    const std::shared_ptr<adbcpq::PostgresTypeResolver>& resolver) {
  if (result.NumColumns() != 6) {
    return Status::Internal(
        "Expected 6 columns from type resolver pg_type query but got ",
        result.NumColumns());
  }

  int num_rows = result.NumRows();
  adbcpq::PostgresTypeResolver::Item type_item;

  for (int row = 0; row < num_rows; row++) {
    adbcpq::PqResultRow item = result.Row(row);
    int64_t oid;
    UNWRAP_RESULT(oid, item[0].ParseInteger());
    const char* typname = item[1].data;
    const char* typreceive = item[2].data;
    int64_t typbasetype;
    UNWRAP_RESULT(typbasetype, item[3].ParseInteger());
    int64_t typrelid;
    UNWRAP_RESULT(typrelid, item[4].ParseInteger());
    int64_t typarray;
    UNWRAP_RESULT(typarray, item[5].ParseInteger());

    if (strcmp(typname, "aclitem") == 0) {
      typreceive = "aclitem_recv";
    }

    type_item.oid = static_cast<uint32_t>(oid);
    type_item.typname = typname;
    type_item.typreceive = typreceive;
    type_item.class_oid = static_cast<uint32_t>(typrelid);
    type_item.base_oid = static_cast<uint32_t>(typbasetype);

    int insert_result = resolver->Insert(type_item, nullptr);

    if (insert_result == NANOARROW_OK && typarray != 0) {
      std::string array_typname = "_" + std::string(typname);
      type_item.oid = static_cast<uint32_t>(typarray);
      type_item.typname = array_typname.c_str();
      type_item.typreceive = "array_recv";
      type_item.child_oid = static_cast<uint32_t>(oid);

      resolver->Insert(type_item, nullptr);
    }
  }

  return Status::Ok();
}

}  // namespace adbchg
