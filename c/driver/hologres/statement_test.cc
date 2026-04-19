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

#include <cstring>
#include <memory>
#include <string>

#include <gtest/gtest.h>
#include <arrow-adbc/adbc.h>

#include "connection.h"
#include "statement.h"

namespace adbchg {

class HologresStatementTest : public ::testing::Test {
 protected:
  void SetUp() override {
    conn_ = std::make_shared<HologresConnection>();
    conn_ptr_ = new std::shared_ptr<HologresConnection>(conn_);
    std::memset(&adbc_conn_, 0, sizeof(adbc_conn_));
    adbc_conn_.private_data = conn_ptr_;

    stmt_ = std::make_unique<HologresStatement>();
    ASSERT_EQ(stmt_->New(&adbc_conn_, &error_), ADBC_STATUS_OK);
  }

  void TearDown() override {
    TearDownError();
    stmt_->Release(&error_);
    TearDownError();
    delete conn_ptr_;
  }

  void TearDownError() {
    if (error_.release) error_.release(&error_);
  }

  std::string GetOptionString(const char* key) {
    char buf[256] = {};
    size_t length = sizeof(buf);
    AdbcStatusCode status = stmt_->GetOption(key, buf, &length, &error_);
    TearDownError();
    if (status != ADBC_STATUS_OK) return "";
    return std::string(buf);
  }

  std::shared_ptr<HologresConnection> conn_;
  std::shared_ptr<HologresConnection>* conn_ptr_;
  struct AdbcConnection adbc_conn_;
  std::unique_ptr<HologresStatement> stmt_;
  struct AdbcError error_ = ADBC_ERROR_INIT;
};

// ---------------------------------------------------------------------------
// New / Release
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, NewWithNullConnection) {
  HologresStatement stmt;
  EXPECT_EQ(stmt.New(nullptr, &error_), ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

TEST_F(HologresStatementTest, NewWithNullPrivateData) {
  HologresStatement stmt;
  struct AdbcConnection conn;
  std::memset(&conn, 0, sizeof(conn));
  conn.private_data = nullptr;
  EXPECT_EQ(stmt.New(&conn, &error_), ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

TEST_F(HologresStatementTest, ReleaseOk) {
  HologresStatement stmt;
  ASSERT_EQ(stmt.New(&adbc_conn_, &error_), ADBC_STATUS_OK);
  TearDownError();
  EXPECT_EQ(stmt.Release(&error_), ADBC_STATUS_OK);
  TearDownError();
}

// ---------------------------------------------------------------------------
// SetSqlQuery / Prepare
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, SetSqlQuery) {
  EXPECT_EQ(stmt_->SetSqlQuery("SELECT 1", &error_), ADBC_STATUS_OK);
  TearDownError();
}

TEST_F(HologresStatementTest, PrepareWithoutQuery) {
  EXPECT_EQ(stmt_->Prepare(&error_), ADBC_STATUS_INVALID_STATE);
  TearDownError();
}

TEST_F(HologresStatementTest, PrepareWithQuery) {
  stmt_->SetSqlQuery("SELECT 1", &error_);
  TearDownError();
  EXPECT_EQ(stmt_->Prepare(&error_), ADBC_STATUS_OK);
  TearDownError();
}

TEST_F(HologresStatementTest, SetSqlQueryClearsIngest) {
  stmt_->SetOption(ADBC_INGEST_OPTION_TARGET_TABLE, "my_table", &error_);
  TearDownError();
  EXPECT_EQ(GetOptionString(ADBC_INGEST_OPTION_TARGET_TABLE), "my_table");

  stmt_->SetSqlQuery("SELECT 1", &error_);
  TearDownError();
  EXPECT_EQ(GetOptionString(ADBC_INGEST_OPTION_TARGET_TABLE), "");
}

// ---------------------------------------------------------------------------
// Bind
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, BindNullArray) {
  struct ArrowSchema schema;
  std::memset(&schema, 0, sizeof(schema));
  schema.release = [](struct ArrowSchema*) {};
  EXPECT_EQ(stmt_->Bind(nullptr, &schema, &error_), ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

TEST_F(HologresStatementTest, BindNullSchema) {
  struct ArrowArray array;
  std::memset(&array, 0, sizeof(array));
  array.release = [](struct ArrowArray*) {};
  EXPECT_EQ(stmt_->Bind(&array, nullptr, &error_), ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

TEST_F(HologresStatementTest, BindStreamNull) {
  EXPECT_EQ(stmt_->Bind(static_cast<struct ArrowArrayStream*>(nullptr), &error_),
            ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

// ---------------------------------------------------------------------------
// SetOption: ADBC_INGEST_OPTION_TARGET_TABLE
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, SetOptionTargetTable) {
  EXPECT_EQ(stmt_->SetOption(ADBC_INGEST_OPTION_TARGET_TABLE, "test_table", &error_),
            ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_INGEST_OPTION_TARGET_TABLE), "test_table");
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionTargetTableClearsQuery) {
  stmt_->SetSqlQuery("SELECT 1", &error_);
  TearDownError();
  stmt_->SetOption(ADBC_INGEST_OPTION_TARGET_TABLE, "my_table", &error_);
  TearDownError();
  // After setting target, prepare should fail because query is cleared
  EXPECT_EQ(stmt_->Prepare(&error_), ADBC_STATUS_INVALID_STATE);
  TearDownError();
}

// ---------------------------------------------------------------------------
// SetOption: ADBC_INGEST_OPTION_TARGET_DB_SCHEMA
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, SetOptionTargetDbSchema) {
  EXPECT_EQ(
      stmt_->SetOption(ADBC_INGEST_OPTION_TARGET_DB_SCHEMA, "my_schema", &error_),
      ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_INGEST_OPTION_TARGET_DB_SCHEMA), "my_schema");
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionTargetDbSchemaClearsWithNull) {
  stmt_->SetOption(ADBC_INGEST_OPTION_TARGET_DB_SCHEMA, "my_schema", &error_);
  TearDownError();
  stmt_->SetOption(ADBC_INGEST_OPTION_TARGET_DB_SCHEMA, nullptr, &error_);
  TearDownError();
  EXPECT_EQ(GetOptionString(ADBC_INGEST_OPTION_TARGET_DB_SCHEMA), "");
}

// ---------------------------------------------------------------------------
// SetOption: ADBC_INGEST_OPTION_MODE
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, SetOptionModeCreate) {
  EXPECT_EQ(
      stmt_->SetOption(ADBC_INGEST_OPTION_MODE, ADBC_INGEST_OPTION_MODE_CREATE, &error_),
      ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_INGEST_OPTION_MODE), ADBC_INGEST_OPTION_MODE_CREATE);
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionModeAppend) {
  EXPECT_EQ(
      stmt_->SetOption(ADBC_INGEST_OPTION_MODE, ADBC_INGEST_OPTION_MODE_APPEND, &error_),
      ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_INGEST_OPTION_MODE), ADBC_INGEST_OPTION_MODE_APPEND);
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionModeReplace) {
  EXPECT_EQ(stmt_->SetOption(ADBC_INGEST_OPTION_MODE, ADBC_INGEST_OPTION_MODE_REPLACE,
                             &error_),
            ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_INGEST_OPTION_MODE), ADBC_INGEST_OPTION_MODE_REPLACE);
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionModeCreateAppend) {
  EXPECT_EQ(stmt_->SetOption(ADBC_INGEST_OPTION_MODE,
                             ADBC_INGEST_OPTION_MODE_CREATE_APPEND, &error_),
            ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_INGEST_OPTION_MODE),
            ADBC_INGEST_OPTION_MODE_CREATE_APPEND);
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionModeInvalid) {
  EXPECT_EQ(stmt_->SetOption(ADBC_INGEST_OPTION_MODE, "invalid_mode", &error_),
            ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

// ---------------------------------------------------------------------------
// SetOption: ADBC_INGEST_OPTION_TEMPORARY
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, SetOptionTemporaryEnabled) {
  EXPECT_EQ(stmt_->SetOption(ADBC_INGEST_OPTION_TEMPORARY, ADBC_OPTION_VALUE_ENABLED,
                             &error_),
            ADBC_STATUS_OK);
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionTemporaryDisabled) {
  EXPECT_EQ(stmt_->SetOption(ADBC_INGEST_OPTION_TEMPORARY, ADBC_OPTION_VALUE_DISABLED,
                             &error_),
            ADBC_STATUS_OK);
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionTemporaryInvalid) {
  EXPECT_EQ(stmt_->SetOption(ADBC_INGEST_OPTION_TEMPORARY, "invalid", &error_),
            ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionTemporaryClearsSchema) {
  stmt_->SetOption(ADBC_INGEST_OPTION_TARGET_DB_SCHEMA, "my_schema", &error_);
  TearDownError();
  stmt_->SetOption(ADBC_INGEST_OPTION_TEMPORARY, ADBC_OPTION_VALUE_ENABLED, &error_);
  TearDownError();
  // Enabling temporary should clear the db_schema
  EXPECT_EQ(GetOptionString(ADBC_INGEST_OPTION_TARGET_DB_SCHEMA), "");
}

// ---------------------------------------------------------------------------
// SetOption: ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, SetOptionBatchSizeValid) {
  EXPECT_EQ(stmt_->SetOption(ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES, "1048576",
                             &error_),
            ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES), "1048576");
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionBatchSizeZero) {
  EXPECT_EQ(
      stmt_->SetOption(ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES, "0", &error_),
      ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionBatchSizeNegative) {
  EXPECT_EQ(
      stmt_->SetOption(ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES, "-1", &error_),
      ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionIntBatchSize) {
  EXPECT_EQ(
      stmt_->SetOptionInt(ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES, 2097152, &error_),
      ADBC_STATUS_OK);
  int64_t val;
  EXPECT_EQ(
      stmt_->GetOptionInt(ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES, &val, &error_),
      ADBC_STATUS_OK);
  EXPECT_EQ(val, 2097152);
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionIntBatchSizeZero) {
  EXPECT_EQ(
      stmt_->SetOptionInt(ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES, 0, &error_),
      ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionIntBatchSizeNegative) {
  EXPECT_EQ(
      stmt_->SetOptionInt(ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES, -100, &error_),
      ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

// ---------------------------------------------------------------------------
// SetOption: ADBC_HOLOGRES_OPTION_USE_COPY
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, SetOptionUseCopyEnabled) {
  EXPECT_EQ(stmt_->SetOption(ADBC_HOLOGRES_OPTION_USE_COPY, ADBC_OPTION_VALUE_ENABLED,
                             &error_),
            ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_HOLOGRES_OPTION_USE_COPY), "true");
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionUseCopyDisabled) {
  EXPECT_EQ(stmt_->SetOption(ADBC_HOLOGRES_OPTION_USE_COPY, ADBC_OPTION_VALUE_DISABLED,
                             &error_),
            ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_HOLOGRES_OPTION_USE_COPY), "false");
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionUseCopyInvalid) {
  EXPECT_EQ(stmt_->SetOption(ADBC_HOLOGRES_OPTION_USE_COPY, "maybe", &error_),
            ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

// ---------------------------------------------------------------------------
// SetOption: ADBC_HOLOGRES_OPTION_ON_CONFLICT
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, SetOptionOnConflictNone) {
  EXPECT_EQ(stmt_->SetOption(ADBC_HOLOGRES_OPTION_ON_CONFLICT, "none", &error_),
            ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_HOLOGRES_OPTION_ON_CONFLICT), "none");
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionOnConflictIgnore) {
  EXPECT_EQ(stmt_->SetOption(ADBC_HOLOGRES_OPTION_ON_CONFLICT, "ignore", &error_),
            ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_HOLOGRES_OPTION_ON_CONFLICT), "ignore");
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionOnConflictUpdate) {
  EXPECT_EQ(stmt_->SetOption(ADBC_HOLOGRES_OPTION_ON_CONFLICT, "update", &error_),
            ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_HOLOGRES_OPTION_ON_CONFLICT), "update");
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionOnConflictInvalid) {
  EXPECT_EQ(stmt_->SetOption(ADBC_HOLOGRES_OPTION_ON_CONFLICT, "merge", &error_),
            ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

// ---------------------------------------------------------------------------
// SetOption: ADBC_HOLOGRES_OPTION_INGEST_MODE
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, SetOptionIngestModeCopy) {
  EXPECT_EQ(stmt_->SetOption(ADBC_HOLOGRES_OPTION_INGEST_MODE, "copy", &error_),
            ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_HOLOGRES_OPTION_INGEST_MODE), "copy");
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionIngestModeStage) {
  EXPECT_EQ(stmt_->SetOption(ADBC_HOLOGRES_OPTION_INGEST_MODE, "stage", &error_),
            ADBC_STATUS_OK);
  EXPECT_EQ(GetOptionString(ADBC_HOLOGRES_OPTION_INGEST_MODE), "stage");
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionIngestModeInvalid) {
  EXPECT_EQ(stmt_->SetOption(ADBC_HOLOGRES_OPTION_INGEST_MODE, "invalid", &error_),
            ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

// ---------------------------------------------------------------------------
// SetOption: unknown key
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, SetOptionUnknownKey) {
  EXPECT_EQ(stmt_->SetOption("unknown.key", "value", &error_),
            ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

// ---------------------------------------------------------------------------
// GetOption: defaults
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, GetOptionDefaultTarget) {
  EXPECT_EQ(GetOptionString(ADBC_INGEST_OPTION_TARGET_TABLE), "");
}

TEST_F(HologresStatementTest, GetOptionDefaultDbSchema) {
  EXPECT_EQ(GetOptionString(ADBC_INGEST_OPTION_TARGET_DB_SCHEMA), "");
}

TEST_F(HologresStatementTest, GetOptionDefaultMode) {
  EXPECT_EQ(GetOptionString(ADBC_INGEST_OPTION_MODE), ADBC_INGEST_OPTION_MODE_CREATE);
}

TEST_F(HologresStatementTest, GetOptionDefaultBatchSize) {
  EXPECT_EQ(GetOptionString(ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES), "16777216");
}

TEST_F(HologresStatementTest, GetOptionDefaultUseCopy) {
  EXPECT_EQ(GetOptionString(ADBC_HOLOGRES_OPTION_USE_COPY), "true");
}

TEST_F(HologresStatementTest, GetOptionDefaultOnConflict) {
  EXPECT_EQ(GetOptionString(ADBC_HOLOGRES_OPTION_ON_CONFLICT), "none");
}

TEST_F(HologresStatementTest, GetOptionDefaultIngestMode) {
  EXPECT_EQ(GetOptionString(ADBC_HOLOGRES_OPTION_INGEST_MODE), "copy");
}

// ---------------------------------------------------------------------------
// GetOption: unknown/unsupported
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, GetOptionUnknownKey) {
  char buf[64];
  size_t length = sizeof(buf);
  EXPECT_EQ(stmt_->GetOption("unknown.key", buf, &length, &error_),
            ADBC_STATUS_NOT_FOUND);
  TearDownError();
}

TEST_F(HologresStatementTest, GetOptionBytesNotFound) {
  uint8_t buf[64];
  size_t length = sizeof(buf);
  EXPECT_EQ(stmt_->GetOptionBytes("any", buf, &length, &error_),
            ADBC_STATUS_NOT_FOUND);
  TearDownError();
}

TEST_F(HologresStatementTest, GetOptionDoubleNotFound) {
  double val;
  EXPECT_EQ(stmt_->GetOptionDouble("any", &val, &error_), ADBC_STATUS_NOT_FOUND);
  TearDownError();
}

TEST_F(HologresStatementTest, GetOptionIntUnknown) {
  int64_t val;
  EXPECT_EQ(stmt_->GetOptionInt("unknown.key", &val, &error_), ADBC_STATUS_NOT_FOUND);
  TearDownError();
}

TEST_F(HologresStatementTest, GetOptionIntBatchSizeDefault) {
  int64_t val;
  EXPECT_EQ(
      stmt_->GetOptionInt(ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES, &val, &error_),
      ADBC_STATUS_OK);
  EXPECT_EQ(val, 16777216);
  TearDownError();
}

// ---------------------------------------------------------------------------
// SetOptionBytes / SetOptionDouble
// ---------------------------------------------------------------------------

TEST_F(HologresStatementTest, SetOptionBytesNotImplemented) {
  uint8_t data[] = {1};
  EXPECT_EQ(stmt_->SetOptionBytes("any", data, 1, &error_),
            ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionDoubleNotImplemented) {
  EXPECT_EQ(stmt_->SetOptionDouble("any", 1.0, &error_), ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

TEST_F(HologresStatementTest, SetOptionIntUnknownKey) {
  EXPECT_EQ(stmt_->SetOptionInt("unknown.key", 42, &error_),
            ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

}  // namespace adbchg
