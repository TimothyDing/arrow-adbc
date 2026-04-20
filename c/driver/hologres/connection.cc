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

#include "connection.h"

#include <array>
#include <cassert>
#include <cinttypes>
#include <cmath>
#include <cstring>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <fmt/format.h>
#include <libpq-fe.h>

#include "database.h"
#include "driver/common/utils.h"
#include "driver/framework/objects.h"
#include "driver/framework/utility.h"
#include "error.h"
#include "result_helper.h"

using adbc::driver::Result;
using adbc::driver::Status;

namespace adbchg {
namespace {

static const uint32_t kSupportedInfoCodes[] = {
    ADBC_INFO_VENDOR_NAME,          ADBC_INFO_VENDOR_VERSION,
    ADBC_INFO_DRIVER_NAME,          ADBC_INFO_DRIVER_VERSION,
    ADBC_INFO_DRIVER_ARROW_VERSION, ADBC_INFO_DRIVER_ADBC_VERSION,
};

static const std::unordered_map<std::string, std::string> kPgTableTypes = {
    {"table", "r"},       {"view", "v"},          {"materialized_view", "m"},
    {"toast_table", "t"}, {"foreign_table", "f"}, {"partitioned_table", "p"}};

static const char* kCatalogQueryAll = "SELECT datname FROM pg_catalog.pg_database";

static const char* kSchemaQueryAll =
    "SELECT nspname FROM pg_catalog.pg_namespace WHERE "
    "nspname !~ '^pg_' AND nspname <> 'information_schema'";

static const char* kTablesQueryAll =
    "SELECT c.relname, CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' "
    "WHEN 'm' THEN 'materialized view' WHEN 't' THEN 'TOAST table' "
    "WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' END "
    "AS reltype FROM pg_catalog.pg_class c "
    "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = $1 AND c.relkind = "
    "ANY($2)";

static const char* kColumnsQueryAll =
    "SELECT attr.attname, attr.attnum, "
    "pg_catalog.col_description(cls.oid, attr.attnum) "
    "FROM pg_catalog.pg_attribute AS attr "
    "INNER JOIN pg_catalog.pg_class AS cls ON attr.attrelid = cls.oid "
    "INNER JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = cls.relnamespace "
    "WHERE attr.attnum > 0 AND NOT attr.attisdropped "
    "AND nsp.nspname LIKE $1 AND cls.relname LIKE $2";

static const char* kConstraintsQueryAll =
    "WITH fk_unnest AS ( "
    "    SELECT "
    "        con.conname, "
    "        'FOREIGN KEY' AS contype, "
    "        conrelid, "
    "        UNNEST(con.conkey) AS conkey, "
    "        confrelid, "
    "        UNNEST(con.confkey) AS confkey "
    "    FROM pg_catalog.pg_constraint AS con "
    "    INNER JOIN pg_catalog.pg_class AS cls ON cls.oid = conrelid "
    "    INNER JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = cls.relnamespace "
    "    WHERE con.contype = 'f' AND nsp.nspname = $1 "
    "    AND cls.relname = $2 "
    "), "
    "fk_names AS ( "
    "    SELECT "
    "        fk_unnest.conname, "
    "        fk_unnest.contype, "
    "        fk_unnest.conkey, "
    "        fk_unnest.confkey, "
    "        attr.attname, "
    "        fnsp.nspname AS fschema, "
    "        fcls.relname AS ftable, "
    "        fattr.attname AS fattname "
    "    FROM fk_unnest "
    "    INNER JOIN pg_catalog.pg_class AS cls ON cls.oid = fk_unnest.conrelid "
    "    INNER JOIN pg_catalog.pg_class AS fcls ON fcls.oid = fk_unnest.confrelid "
    "    INNER JOIN pg_catalog.pg_namespace AS fnsp ON fnsp.oid = fcls.relnamespace"
    "    INNER JOIN pg_catalog.pg_attribute AS attr ON attr.attnum = "
    "fk_unnest.conkey "
    "        AND attr.attrelid = fk_unnest.conrelid "
    "    LEFT JOIN pg_catalog.pg_attribute AS fattr ON fattr.attnum =  "
    "fk_unnest.confkey "
    "        AND fattr.attrelid = fk_unnest.confrelid "
    "), "
    "fkeys AS ( "
    "    SELECT "
    "        conname, "
    "        contype, "
    "        ARRAY_AGG(attname ORDER BY conkey) AS colnames, "
    "        fschema, "
    "        ftable, "
    "        ARRAY_AGG(fattname ORDER BY confkey) AS fcolnames "
    "    FROM fk_names "
    "    GROUP BY "
    "        conname, "
    "        contype, "
    "        fschema, "
    "        ftable "
    "), "
    "other_constraints AS ( "
    "    SELECT con.conname, CASE con.contype WHEN 'c' THEN 'CHECK' WHEN 'u' THEN  "
    "    'UNIQUE' WHEN 'p' THEN 'PRIMARY KEY' END AS contype, "
    "    ARRAY_AGG(attr.attname) AS colnames "
    "    FROM pg_catalog.pg_constraint AS con  "
    "    CROSS JOIN UNNEST(conkey) AS conkeys  "
    "    INNER JOIN pg_catalog.pg_class AS cls ON cls.oid = con.conrelid  "
    "    INNER JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = cls.relnamespace  "
    "    INNER JOIN pg_catalog.pg_attribute AS attr ON attr.attnum = conkeys  "
    "    AND cls.oid = attr.attrelid  "
    "    WHERE con.contype IN ('c', 'u', 'p') AND nsp.nspname = $1 "
    "    AND cls.relname = $2 "
    "    GROUP BY conname, contype "
    ") "
    "SELECT "
    "    conname, contype, colnames, fschema, ftable, fcolnames "
    "FROM fkeys "
    "UNION ALL "
    "SELECT "
    "    conname, contype, colnames, NULL, NULL, NULL "
    "FROM other_constraints";

class HologresGetObjectsHelper : public adbc::driver::GetObjectsHelper {
 public:
  explicit HologresGetObjectsHelper(PGconn* conn)
      : current_database_(PQdb(conn)),
        all_catalogs_(conn, kCatalogQueryAll),
        some_catalogs_(conn, CatalogQuery()),
        all_schemas_(conn, kSchemaQueryAll),
        some_schemas_(conn, SchemaQuery()),
        all_tables_(conn, kTablesQueryAll),
        some_tables_(conn, TablesQuery()),
        all_columns_(conn, kColumnsQueryAll),
        some_columns_(conn, ColumnsQuery()),
        all_constraints_(conn, kConstraintsQueryAll),
        some_constraints_(conn, ConstraintsQuery()) {}

  Status Load(adbc::driver::GetObjectsDepth depth,
              std::optional<std::string_view> catalog_filter,
              std::optional<std::string_view> schema_filter,
              std::optional<std::string_view> table_filter,
              std::optional<std::string_view> column_filter,
              const std::vector<std::string_view>& table_types) override {
    return Status::Ok();
  }

  Status LoadCatalogs(std::optional<std::string_view> catalog_filter) override {
    if (catalog_filter.has_value()) {
      UNWRAP_STATUS(some_catalogs_.Execute({std::string(*catalog_filter)}));
      next_catalog_ = some_catalogs_.Row(-1);
    } else {
      UNWRAP_STATUS(all_catalogs_.Execute());
      next_catalog_ = all_catalogs_.Row(-1);
    }
    return Status::Ok();
  };

  Result<std::optional<std::string_view>> NextCatalog() override {
    next_catalog_ = next_catalog_.Next();
    if (!next_catalog_.IsValid()) {
      return std::nullopt;
    }
    return next_catalog_[0].value();
  }

  Status LoadSchemas(std::string_view catalog,
                     std::optional<std::string_view> schema_filter) override {
    if (catalog != current_database_) {
      return Status::Ok();
    }

    if (schema_filter.has_value()) {
      UNWRAP_STATUS(some_schemas_.Execute({std::string(*schema_filter)}));
      next_schema_ = some_schemas_.Row(-1);
    } else {
      UNWRAP_STATUS(all_schemas_.Execute());
      next_schema_ = all_schemas_.Row(-1);
    }
    return Status::Ok();
  };

  Result<std::optional<std::string_view>> NextSchema() override {
    next_schema_ = next_schema_.Next();
    if (!next_schema_.IsValid()) {
      return std::nullopt;
    }
    return next_schema_[0].value();
  }

  Status LoadTables(std::string_view catalog, std::string_view schema,
                    std::optional<std::string_view> table_filter,
                    const std::vector<std::string_view>& table_types) override {
    std::string table_types_bind = TableTypesArrayLiteral(table_types);

    if (table_filter.has_value()) {
      UNWRAP_STATUS(some_tables_.Execute(
          {std::string(schema), table_types_bind, std::string(*table_filter)}));
      next_table_ = some_tables_.Row(-1);
    } else {
      UNWRAP_STATUS(all_tables_.Execute({std::string(schema), table_types_bind}));
      next_table_ = all_tables_.Row(-1);
    }
    return Status::Ok();
  };

  Result<std::optional<Table>> NextTable() override {
    next_table_ = next_table_.Next();
    if (!next_table_.IsValid()) {
      return std::nullopt;
    }
    return Table{next_table_[0].value(), next_table_[1].value()};
  }

  Status LoadColumns(std::string_view catalog, std::string_view schema,
                     std::string_view table,
                     std::optional<std::string_view> column_filter) override {
    if (column_filter.has_value()) {
      UNWRAP_STATUS(some_columns_.Execute(
          {std::string(schema), std::string(table), std::string(*column_filter)}));
      next_column_ = some_columns_.Row(-1);
    } else {
      UNWRAP_STATUS(all_columns_.Execute({std::string(schema), std::string(table)}));
      next_column_ = all_columns_.Row(-1);
    }

    if (column_filter.has_value()) {
      UNWRAP_STATUS(some_constraints_.Execute(
          {std::string(schema), std::string(table), std::string(*column_filter)}))
      next_constraint_ = some_constraints_.Row(-1);
    } else {
      UNWRAP_STATUS(
          all_constraints_.Execute({std::string(schema), std::string(table)}));
      next_constraint_ = all_constraints_.Row(-1);
    }

    return Status::Ok();
  };

  Result<std::optional<Column>> NextColumn() override {
    next_column_ = next_column_.Next();
    if (!next_column_.IsValid()) {
      return std::nullopt;
    }

    Column col;
    col.column_name = next_column_[0].value();
    int64_t ordinal_position;
    UNWRAP_RESULT(ordinal_position, next_column_[1].ParseInteger());
    col.ordinal_position = static_cast<int32_t>(ordinal_position);
    if (!next_column_[2].is_null) {
      col.remarks = next_column_[2].value();
    }
    return col;
  }

  Result<std::optional<Constraint>> NextConstraint() override {
    next_constraint_ = next_constraint_.Next();
    if (!next_constraint_.IsValid()) {
      return std::nullopt;
    }

    Constraint out;
    out.name = next_constraint_[0].data;
    out.type = next_constraint_[1].data;

    UNWRAP_RESULT(constraint_fcolumn_names_, next_constraint_[2].ParseTextArray());
    std::vector<std::string_view> fcolumn_names_view;
    for (const std::string& item : constraint_fcolumn_names_) {
      fcolumn_names_view.push_back(item);
    }
    out.column_names = std::move(fcolumn_names_view);

    if (out.type == "FOREIGN KEY") {
      assert(!next_constraint_[3].is_null);
      assert(!next_constraint_[4].is_null);
      assert(!next_constraint_[5].is_null);

      out.usage = std::vector<ConstraintUsage>();
      UNWRAP_RESULT(constraint_fkey_names_, next_constraint_[5].ParseTextArray());

      for (const auto& item : constraint_fkey_names_) {
        ConstraintUsage usage;
        usage.catalog = current_database_;
        usage.schema = next_constraint_[3].data;
        usage.table = next_constraint_[4].data;
        usage.column = item;
        out.usage->push_back(usage);
      }
    }

    return out;
  }

 private:
  std::string current_database_;

  adbcpq::PqResultHelper all_catalogs_;
  adbcpq::PqResultHelper some_catalogs_;
  adbcpq::PqResultHelper all_schemas_;
  adbcpq::PqResultHelper some_schemas_;
  adbcpq::PqResultHelper all_tables_;
  adbcpq::PqResultHelper some_tables_;
  adbcpq::PqResultHelper all_columns_;
  adbcpq::PqResultHelper some_columns_;
  adbcpq::PqResultHelper all_constraints_;
  adbcpq::PqResultHelper some_constraints_;

  adbcpq::PqResultRow next_catalog_;
  adbcpq::PqResultRow next_schema_;
  adbcpq::PqResultRow next_table_;
  adbcpq::PqResultRow next_column_;
  adbcpq::PqResultRow next_constraint_;

  std::vector<std::string> constraint_fcolumn_names_;
  std::vector<std::string> constraint_fkey_names_;

  static std::string CatalogQuery() {
    return std::string(kCatalogQueryAll) + " WHERE datname = $1";
  }

  static std::string SchemaQuery() {
    return std::string(kSchemaQueryAll) + " AND nspname = $1";
  }

  static std::string TablesQuery() {
    return std::string(kTablesQueryAll) + " AND c.relname LIKE $3";
  }

  static std::string ColumnsQuery() {
    return std::string(kColumnsQueryAll) + " AND attr.attname LIKE $3";
  }

  static std::string ConstraintsQuery() {
    return std::string(kConstraintsQueryAll) + " WHERE conname LIKE $3";
  }

  std::string TableTypesArrayLiteral(const std::vector<std::string_view>& table_types) {
    std::stringstream table_types_bind;
    table_types_bind << "{";
    int table_types_bind_len = 0;

    if (table_types.empty()) {
      for (const auto& item : kPgTableTypes) {
        if (table_types_bind_len > 0) {
          table_types_bind << ", ";
        }
        table_types_bind << "\"" << item.second << "\"";
        table_types_bind_len++;
      }
    } else {
      for (auto type : table_types) {
        const auto maybe_item = kPgTableTypes.find(std::string(type));
        if (maybe_item == kPgTableTypes.end()) {
          continue;
        }
        if (table_types_bind_len > 0) {
          table_types_bind << ", ";
        }
        table_types_bind << "\"" << maybe_item->second << "\"";
        table_types_bind_len++;
      }
    }

    table_types_bind << "}";
    return table_types_bind.str();
  }
};

void SilentNoticeProcessor(void* /*arg*/, const char* /*message*/) {}

}  // namespace

AdbcStatusCode HologresConnection::Cancel(struct AdbcError* error) {
  char errbuf[256];
  if (PQcancel(cancel_, errbuf, sizeof(errbuf)) != 1) {
    InternalAdbcSetError(error, "[hologres] Failed to cancel operation: %s", errbuf);
    return ADBC_STATUS_UNKNOWN;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnection::Commit(struct AdbcError* error) {
  InternalAdbcSetError(error, "%s",
                       "[hologres] Hologres does not support transactions");
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::GetInfo(struct AdbcConnection* connection,
                                           const uint32_t* info_codes,
                                           size_t info_codes_length,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  if (!info_codes) {
    info_codes = kSupportedInfoCodes;
    info_codes_length = sizeof(kSupportedInfoCodes) / sizeof(kSupportedInfoCodes[0]);
  }

  std::vector<adbc::driver::InfoValue> infos;

  for (size_t i = 0; i < info_codes_length; i++) {
    switch (info_codes[i]) {
      case ADBC_INFO_VENDOR_NAME:
        infos.push_back({info_codes[i], std::string("Hologres")});
        break;
      case ADBC_INFO_VENDOR_VERSION: {
        const std::array<int, 3>& version = database_->VendorVersion();
        std::string version_string = std::to_string(version[0]) + "." +
                                     std::to_string(version[1]) + "." +
                                     std::to_string(version[2]);
        infos.push_back({info_codes[i], std::move(version_string)});
        break;
      }
      case ADBC_INFO_DRIVER_NAME:
        infos.push_back({info_codes[i], "ADBC Hologres Driver"});
        break;
      case ADBC_INFO_DRIVER_VERSION:
        infos.push_back({info_codes[i], ADBC_HOLOGRES_VERSION});
        break;
      case ADBC_INFO_DRIVER_ARROW_VERSION:
        infos.push_back({info_codes[i], "v" NANOARROW_VERSION});
        break;
      case ADBC_INFO_DRIVER_ADBC_VERSION:
        infos.push_back({info_codes[i], ADBC_VERSION_1_1_0});
        break;
      default:
        continue;
    }
  }

  RAISE_ADBC(adbc::driver::MakeGetInfoStream(infos, out).ToAdbc(error));
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnection::GetObjects(
    struct AdbcConnection* connection, int c_depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_type,
    const char* column_name, struct ArrowArrayStream* out, struct AdbcError* error) {
  HologresGetObjectsHelper helper(conn_);

  const auto catalog_filter =
      catalog ? std::make_optional(std::string_view(catalog)) : std::nullopt;
  const auto schema_filter =
      db_schema ? std::make_optional(std::string_view(db_schema)) : std::nullopt;
  const auto table_filter =
      table_name ? std::make_optional(std::string_view(table_name)) : std::nullopt;
  const auto column_filter =
      column_name ? std::make_optional(std::string_view(column_name)) : std::nullopt;
  std::vector<std::string_view> table_type_filter;
  while (table_type && *table_type) {
    if (*table_type) {
      table_type_filter.push_back(std::string_view(*table_type));
    }
    table_type++;
  }

  using adbc::driver::GetObjectsDepth;

  GetObjectsDepth depth = GetObjectsDepth::kColumns;
  switch (c_depth) {
    case ADBC_OBJECT_DEPTH_CATALOGS:
      depth = GetObjectsDepth::kCatalogs;
      break;
    case ADBC_OBJECT_DEPTH_COLUMNS:
      depth = GetObjectsDepth::kColumns;
      break;
    case ADBC_OBJECT_DEPTH_DB_SCHEMAS:
      depth = GetObjectsDepth::kSchemas;
      break;
    case ADBC_OBJECT_DEPTH_TABLES:
      depth = GetObjectsDepth::kTables;
      break;
    default:
      return Status::InvalidArgument("[hologres] GetObjects: invalid depth ", c_depth)
          .ToAdbc(error);
  }

  auto status = BuildGetObjects(&helper, depth, catalog_filter, schema_filter,
                                table_filter, column_filter, table_type_filter, out);
  RAISE_STATUS(error, helper.Close());
  RAISE_STATUS(error, status);

  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnection::GetOption(const char* option, char* value,
                                             size_t* length, struct AdbcError* error) {
  std::string output;
  if (std::strcmp(option, ADBC_CONNECTION_OPTION_CURRENT_CATALOG) == 0) {
    output = PQdb(conn_);
  } else if (std::strcmp(option, ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA) == 0) {
    adbcpq::PqResultHelper result_helper{conn_, "SELECT CURRENT_SCHEMA()"};
    RAISE_STATUS(error, result_helper.Execute());
    auto it = result_helper.begin();
    if (it == result_helper.end()) {
      InternalAdbcSetError(
          error,
          "[hologres] Hologres returned no rows for 'SELECT CURRENT_SCHEMA()'");
      return ADBC_STATUS_INTERNAL;
    }
    output = (*it)[0].data;
  } else if (std::strcmp(option, ADBC_CONNECTION_OPTION_AUTOCOMMIT) == 0) {
    output = ADBC_OPTION_VALUE_ENABLED;
  } else {
    return ADBC_STATUS_NOT_FOUND;
  }

  if (output.size() + 1 <= *length) {
    std::memcpy(value, output.c_str(), output.size() + 1);
  }
  *length = output.size() + 1;
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnection::GetOptionBytes(const char* option, uint8_t* value,
                                                  size_t* length,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresConnection::GetOptionInt(const char* option, int64_t* value,
                                                struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresConnection::GetOptionDouble(const char* option, double* value,
                                                   struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresConnectionGetStatisticsImpl(PGconn* conn, const char* db_schema,
                                                   const char* table_name,
                                                   struct ArrowSchema* schema,
                                                   struct ArrowArray* array,
                                                   struct AdbcError* error) {
  auto uschema = nanoarrow::UniqueSchema();
  {
    ArrowSchemaInit(uschema.get());
    CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(uschema.get(), /*num_columns=*/2), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetType(uschema->children[0], NANOARROW_TYPE_STRING),
             error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(uschema->children[0], "catalog_name"), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetType(uschema->children[1], NANOARROW_TYPE_LIST),
             error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(uschema->children[1], "catalog_db_schemas"),
             error);
    CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(uschema->children[1]->children[0], 2),
             error);
    uschema->children[1]->flags &= ~ARROW_FLAG_NULLABLE;

    struct ArrowSchema* db_schema_schema = uschema->children[1]->children[0];
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(db_schema_schema->children[0], NANOARROW_TYPE_STRING),
             error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetName(db_schema_schema->children[0], "db_schema_name"), error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(db_schema_schema->children[1], NANOARROW_TYPE_LIST),
             error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetName(db_schema_schema->children[1], "db_schema_statistics"),
             error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetTypeStruct(db_schema_schema->children[1]->children[0], 5),
             error);
    db_schema_schema->children[1]->flags &= ~ARROW_FLAG_NULLABLE;

    struct ArrowSchema* statistics_schema = db_schema_schema->children[1]->children[0];
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(statistics_schema->children[0], NANOARROW_TYPE_STRING),
             error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(statistics_schema->children[0], "table_name"),
             error);
    statistics_schema->children[0]->flags &= ~ARROW_FLAG_NULLABLE;
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(statistics_schema->children[1], NANOARROW_TYPE_STRING),
             error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(statistics_schema->children[1], "column_name"),
             error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(statistics_schema->children[2], NANOARROW_TYPE_INT16),
             error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetName(statistics_schema->children[2], "statistic_key"), error);
    statistics_schema->children[2]->flags &= ~ARROW_FLAG_NULLABLE;
    CHECK_NA(INTERNAL,
             ArrowSchemaSetTypeUnion(statistics_schema->children[3],
                                     NANOARROW_TYPE_DENSE_UNION, 4),
             error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetName(statistics_schema->children[3], "statistic_value"),
             error);
    statistics_schema->children[3]->flags &= ~ARROW_FLAG_NULLABLE;
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(statistics_schema->children[4], NANOARROW_TYPE_BOOL),
             error);
    CHECK_NA(
        INTERNAL,
        ArrowSchemaSetName(statistics_schema->children[4], "statistic_is_approximate"),
        error);
    statistics_schema->children[4]->flags &= ~ARROW_FLAG_NULLABLE;

    struct ArrowSchema* value_schema = statistics_schema->children[3];
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(value_schema->children[0], NANOARROW_TYPE_INT64), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(value_schema->children[0], "int64"), error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(value_schema->children[1], NANOARROW_TYPE_UINT64), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(value_schema->children[1], "uint64"), error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(value_schema->children[2], NANOARROW_TYPE_DOUBLE), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(value_schema->children[2], "float64"), error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(value_schema->children[3], NANOARROW_TYPE_BINARY), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(value_schema->children[3], "binary"), error);
  }

  struct ArrowError na_error = {0};
  CHECK_NA_DETAIL(INTERNAL, ArrowArrayInitFromSchema(array, uschema.get(), &na_error),
                  &na_error, error);
  CHECK_NA(INTERNAL, ArrowArrayStartAppending(array), error);

  struct ArrowArray* catalog_name_col = array->children[0];
  struct ArrowArray* catalog_db_schemas_col = array->children[1];
  struct ArrowArray* catalog_db_schemas_items = catalog_db_schemas_col->children[0];
  struct ArrowArray* db_schema_name_col = catalog_db_schemas_items->children[0];
  struct ArrowArray* db_schema_statistics_col = catalog_db_schemas_items->children[1];
  struct ArrowArray* db_schema_statistics_items = db_schema_statistics_col->children[0];
  struct ArrowArray* statistics_table_name_col = db_schema_statistics_items->children[0];
  struct ArrowArray* statistics_column_name_col = db_schema_statistics_items->children[1];
  struct ArrowArray* statistics_key_col = db_schema_statistics_items->children[2];
  struct ArrowArray* statistics_value_col = db_schema_statistics_items->children[3];
  struct ArrowArray* statistics_is_approximate_col =
      db_schema_statistics_items->children[4];
  struct ArrowArray* value_float64_col = statistics_value_col->children[2];

  std::string query = R"(
    WITH
      class AS (
        SELECT nspname, relname, reltuples
        FROM pg_namespace
        INNER JOIN pg_class ON pg_class.relnamespace = pg_namespace.oid
      )
    SELECT tablename, attname, null_frac, avg_width, n_distinct, reltuples
    FROM pg_stats
    INNER JOIN class ON pg_stats.schemaname = class.nspname AND pg_stats.tablename = class.relname
    WHERE pg_stats.schemaname = $1 AND tablename LIKE $2
    ORDER BY tablename
)";

  CHECK_NA(INTERNAL, ArrowArrayAppendString(catalog_name_col, ArrowCharView(PQdb(conn))),
           error);
  CHECK_NA(INTERNAL, ArrowArrayAppendString(db_schema_name_col, ArrowCharView(db_schema)),
           error);

  constexpr int8_t kStatsVariantFloat64 = 2;

  std::string prev_table;

  {
    adbcpq::PqResultHelper result_helper{conn, query};
    RAISE_STATUS(error,
                 result_helper.Execute({db_schema, table_name ? table_name : "%"}));

    for (adbcpq::PqResultRow row : result_helper) {
      auto reltuples = row[5].ParseDouble();
      if (!reltuples) {
        InternalAdbcSetError(error, "[hologres] Invalid double value in reltuples: '%s'",
                             row[5].data);
        return ADBC_STATUS_INTERNAL;
      }

      if (std::strcmp(prev_table.c_str(), row[0].data) != 0) {
        CHECK_NA(INTERNAL,
                 ArrowArrayAppendString(statistics_table_name_col,
                                        ArrowStringView{row[0].data, row[0].len}),
                 error);
        CHECK_NA(INTERNAL, ArrowArrayAppendNull(statistics_column_name_col, 1), error);
        CHECK_NA(INTERNAL,
                 ArrowArrayAppendInt(statistics_key_col, ADBC_STATISTIC_ROW_COUNT_KEY),
                 error);
        CHECK_NA(INTERNAL, ArrowArrayAppendDouble(value_float64_col, *reltuples), error);
        CHECK_NA(INTERNAL,
                 ArrowArrayFinishUnionElement(statistics_value_col, kStatsVariantFloat64),
                 error);
        CHECK_NA(INTERNAL, ArrowArrayAppendInt(statistics_is_approximate_col, 1), error);
        CHECK_NA(INTERNAL, ArrowArrayFinishElement(db_schema_statistics_items), error);
        prev_table = std::string(row[0].data, row[0].len);
      }

      auto null_frac = row[2].ParseDouble();
      if (!null_frac) {
        InternalAdbcSetError(error, "[hologres] Invalid double value in null_frac: '%s'",
                             row[2].data);
        return ADBC_STATUS_INTERNAL;
      }

      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(statistics_table_name_col,
                                      ArrowStringView{row[0].data, row[0].len}),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(statistics_column_name_col,
                                      ArrowStringView{row[1].data, row[1].len}),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendInt(statistics_key_col, ADBC_STATISTIC_NULL_COUNT_KEY),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendDouble(value_float64_col, *null_frac * *reltuples), error);
      CHECK_NA(INTERNAL,
               ArrowArrayFinishUnionElement(statistics_value_col, kStatsVariantFloat64),
               error);
      CHECK_NA(INTERNAL, ArrowArrayAppendInt(statistics_is_approximate_col, 1), error);
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(db_schema_statistics_items), error);

      auto average_byte_width = row[3].ParseDouble();
      if (!average_byte_width) {
        InternalAdbcSetError(error, "[hologres] Invalid double value in avg_width: '%s'",
                             row[3].data);
        return ADBC_STATUS_INTERNAL;
      }

      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(statistics_table_name_col,
                                      ArrowStringView{row[0].data, row[0].len}),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(statistics_column_name_col,
                                      ArrowStringView{row[1].data, row[1].len}),
               error);
      CHECK_NA(
          INTERNAL,
          ArrowArrayAppendInt(statistics_key_col, ADBC_STATISTIC_AVERAGE_BYTE_WIDTH_KEY),
          error);
      CHECK_NA(INTERNAL, ArrowArrayAppendDouble(value_float64_col, *average_byte_width),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayFinishUnionElement(statistics_value_col, kStatsVariantFloat64),
               error);
      CHECK_NA(INTERNAL, ArrowArrayAppendInt(statistics_is_approximate_col, 1), error);
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(db_schema_statistics_items), error);

      auto n_distinct = row[4].ParseDouble();
      if (!n_distinct) {
        InternalAdbcSetError(error, "[hologres] Invalid double value in n_distinct: '%s'",
                             row[4].data);
        return ADBC_STATUS_INTERNAL;
      }

      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(statistics_table_name_col,
                                      ArrowStringView{row[0].data, row[0].len}),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(statistics_column_name_col,
                                      ArrowStringView{row[1].data, row[1].len}),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendInt(statistics_key_col, ADBC_STATISTIC_DISTINCT_COUNT_KEY),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendDouble(
                   value_float64_col,
                   *n_distinct > 0 ? *n_distinct : (std::fabs(*n_distinct) * *reltuples)),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayFinishUnionElement(statistics_value_col, kStatsVariantFloat64),
               error);
      CHECK_NA(INTERNAL, ArrowArrayAppendInt(statistics_is_approximate_col, 1), error);
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(db_schema_statistics_items), error);
    }
  }

  CHECK_NA(INTERNAL, ArrowArrayFinishElement(db_schema_statistics_col), error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(catalog_db_schemas_items), error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(catalog_db_schemas_col), error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);

  CHECK_NA_DETAIL(INTERNAL, ArrowArrayFinishBuildingDefault(array, &na_error), &na_error,
                  error);
  uschema.move(schema);
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnection::GetStatistics(const char* catalog,
                                                 const char* db_schema,
                                                 const char* table_name, bool approximate,
                                                 struct ArrowArrayStream* out,
                                                 struct AdbcError* error) {
  if (!approximate) {
    InternalAdbcSetError(error, "[hologres] Exact statistics are not implemented");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  } else if (!db_schema) {
    InternalAdbcSetError(error, "[hologres] Must request statistics for a single schema");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  } else if (catalog && std::strcmp(catalog, PQdb(conn_)) != 0) {
    InternalAdbcSetError(error,
                         "[hologres] Can only request statistics for current catalog");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  struct ArrowSchema schema;
  std::memset(&schema, 0, sizeof(schema));
  struct ArrowArray array;
  std::memset(&array, 0, sizeof(array));

  AdbcStatusCode status = HologresConnectionGetStatisticsImpl(
      conn_, db_schema, table_name, &schema, &array, error);
  if (status != ADBC_STATUS_OK) {
    if (schema.release) schema.release(&schema);
    if (array.release) array.release(&array);
    return status;
  }

  adbc::driver::MakeArrayStream(&schema, &array, out);
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnectionGetStatisticNamesImpl(struct ArrowSchema* schema,
                                                       struct ArrowArray* array,
                                                       struct AdbcError* error) {
  auto uschema = nanoarrow::UniqueSchema();
  ArrowSchemaInit(uschema.get());

  CHECK_NA(INTERNAL, ArrowSchemaSetType(uschema.get(), NANOARROW_TYPE_STRUCT), error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(uschema.get(), /*num_columns=*/2),
           error);

  ArrowSchemaInit(uschema.get()->children[0]);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(uschema.get()->children[0], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(uschema.get()->children[0], "statistic_name"),
           error);
  uschema.get()->children[0]->flags &= ~ARROW_FLAG_NULLABLE;

  ArrowSchemaInit(uschema.get()->children[1]);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(uschema.get()->children[1], NANOARROW_TYPE_INT16),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(uschema.get()->children[1], "statistic_key"),
           error);
  uschema.get()->children[1]->flags &= ~ARROW_FLAG_NULLABLE;

  CHECK_NA(INTERNAL, ArrowArrayInitFromSchema(array, uschema.get(), NULL), error);
  CHECK_NA(INTERNAL, ArrowArrayStartAppending(array), error);
  CHECK_NA(INTERNAL, ArrowArrayFinishBuildingDefault(array, NULL), error);

  uschema.move(schema);
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnection::GetStatisticNames(struct ArrowArrayStream* out,
                                                     struct AdbcError* error) {
  struct ArrowSchema schema;
  std::memset(&schema, 0, sizeof(schema));
  struct ArrowArray array;
  std::memset(&array, 0, sizeof(array));

  AdbcStatusCode status = HologresConnectionGetStatisticNamesImpl(&schema, &array, error);
  if (status != ADBC_STATUS_OK) {
    if (schema.release) schema.release(&schema);
    if (array.release) array.release(&array);
    return status;
  }

  adbc::driver::MakeArrayStream(&schema, &array, out);
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnection::GetTableSchema(const char* catalog,
                                                  const char* db_schema,
                                                  const char* table_name,
                                                  struct ArrowSchema* schema,
                                                  struct AdbcError* error) {
  AdbcStatusCode final_status = ADBC_STATUS_OK;

  char* quoted = PQescapeIdentifier(conn_, table_name, strlen(table_name));
  std::string table_name_str(quoted);
  PQfreemem(quoted);

  if (db_schema != nullptr) {
    quoted = PQescapeIdentifier(conn_, db_schema, strlen(db_schema));
    table_name_str = std::string(quoted) + "." + table_name_str;
    PQfreemem(quoted);
  }

  std::string query =
      "SELECT attname, atttypid "
      "FROM pg_catalog.pg_class AS cls "
      "INNER JOIN pg_catalog.pg_attribute AS attr ON cls.oid = attr.attrelid "
      "INNER JOIN pg_catalog.pg_type AS typ ON attr.atttypid = typ.oid "
      "WHERE attr.attnum >= 0 AND cls.oid = $1::regclass::oid "
      "ORDER BY attr.attnum";

  std::vector<std::string> params = {table_name_str};

  adbcpq::PqResultHelper result_helper =
      adbcpq::PqResultHelper{conn_, std::string(query.c_str())};

  RAISE_STATUS(error, result_helper.Execute(params));

  auto uschema = nanoarrow::UniqueSchema();
  ArrowSchemaInit(uschema.get());
  CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(uschema.get(), result_helper.NumRows()),
           error);

  int row_counter = 0;
  for (auto row : result_helper) {
    const char* colname = row[0].data;
    const Oid pg_oid =
        static_cast<uint32_t>(std::strtol(row[1].data, /*str_end=*/nullptr, /*base=*/10));

    adbcpq::PostgresType pg_type;
    if (type_resolver_->FindWithDefault(pg_oid, &pg_type) != NANOARROW_OK) {
      InternalAdbcSetError(error, "%s%d%s%s%s%" PRIu32,
                           "Error resolving type code for column #", row_counter + 1,
                           " (\"", colname, "\")  with oid ", pg_oid);
      final_status = ADBC_STATUS_NOT_IMPLEMENTED;
      break;
    }
    CHECK_NA(INTERNAL,
             pg_type.WithFieldName(colname).SetSchema(uschema->children[row_counter],
                                                      std::string("Hologres")),
             error);
    row_counter++;
  }
  uschema.move(schema);

  return final_status;
}

AdbcStatusCode HologresConnection::GetTableTypes(struct AdbcConnection* connection,
                                                 struct ArrowArrayStream* out,
                                                 struct AdbcError* error) {
  std::vector<std::string> table_types;
  table_types.reserve(kPgTableTypes.size());
  for (auto const& table_type : kPgTableTypes) {
    table_types.push_back(table_type.first);
  }

  RAISE_STATUS(error, adbc::driver::MakeTableTypesStream(table_types, out));
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnection::Init(struct AdbcDatabase* database,
                                        struct AdbcError* error) {
  if (!database || !database->private_data) {
    InternalAdbcSetError(error, "[hologres] Must provide an initialized AdbcDatabase");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  database_ =
      *reinterpret_cast<std::shared_ptr<HologresDatabase>*>(database->private_data);
  type_resolver_ = database_->type_resolver();

  RAISE_ADBC(database_->Connect(&conn_, error));

  cancel_ = PQgetCancel(conn_);
  if (!cancel_) {
    InternalAdbcSetError(error, "[hologres] Could not initialize PGcancel");
    return ADBC_STATUS_UNKNOWN;
  }

  std::ignore = PQsetNoticeProcessor(conn_, SilentNoticeProcessor, nullptr);

  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnection::Release(struct AdbcError* error) {
  if (cancel_) {
    PQfreeCancel(cancel_);
    cancel_ = nullptr;
  }
  if (conn_) {
    return database_->Disconnect(&conn_, error);
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresConnection::Rollback(struct AdbcError* error) {
  InternalAdbcSetError(error, "%s",
                       "[hologres] Hologres does not support transactions");
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
  } else if (std::strcmp(key, ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA) == 0) {
    char* value_esc = PQescapeIdentifier(conn_, value, strlen(value));
    if (!value_esc) {
      InternalAdbcSetError(error, "[hologres] Could not escape identifier: %s",
                           PQerrorMessage(conn_));
      return ADBC_STATUS_INTERNAL;
    }
    std::string query = fmt::format("SET search_path TO {}", value_esc);
    PQfreemem(value_esc);

    adbcpq::PqResultHelper result_helper{conn_, query};
    RAISE_STATUS(error, result_helper.Execute());
    return ADBC_STATUS_OK;
  }
  InternalAdbcSetError(error, "[hologres] Unknown connection option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::SetOptionBytes(const char* key, const uint8_t* value,
                                                  size_t length,
                                                  struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown connection option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::SetOptionDouble(const char* key, double value,
                                                   struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown connection option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresConnection::SetOptionInt(const char* key, int64_t value,
                                                struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown connection option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

}  // namespace adbchg
