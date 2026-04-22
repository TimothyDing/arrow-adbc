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
#include <set>
#include <string>

#include <gtest/gtest.h>
#include <arrow-adbc/adbc.h>

#include "connection.h"

namespace adbchg {

class HologresConnectionTest : public ::testing::Test {
 protected:
  void SetUp() override { conn_ = std::make_unique<HologresConnection>(); }

  std::unique_ptr<HologresConnection> conn_;
  struct AdbcError error_ = ADBC_ERROR_INIT;

  void TearDownError() {
    if (error_.release) error_.release(&error_);
  }
};

TEST_F(HologresConnectionTest, ConstructorDefaults) {
  EXPECT_EQ(conn_->conn(), nullptr);
  EXPECT_TRUE(conn_->autocommit());
  EXPECT_EQ(conn_->database(), nullptr);
}

TEST_F(HologresConnectionTest, CommitReturnsNotImplemented) {
  EXPECT_EQ(conn_->Commit(&error_), ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

TEST_F(HologresConnectionTest, RollbackReturnsNotImplemented) {
  EXPECT_EQ(conn_->Rollback(&error_), ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

TEST_F(HologresConnectionTest, GetOptionBytesReturnsNotFound) {
  uint8_t buf[64];
  size_t length = sizeof(buf);
  EXPECT_EQ(conn_->GetOptionBytes("any", buf, &length, &error_), ADBC_STATUS_NOT_FOUND);
  TearDownError();
}

TEST_F(HologresConnectionTest, GetOptionIntReturnsNotFound) {
  int64_t val;
  EXPECT_EQ(conn_->GetOptionInt("any", &val, &error_), ADBC_STATUS_NOT_FOUND);
  TearDownError();
}

TEST_F(HologresConnectionTest, GetOptionDoubleReturnsNotFound) {
  double val;
  EXPECT_EQ(conn_->GetOptionDouble("any", &val, &error_), ADBC_STATUS_NOT_FOUND);
  TearDownError();
}

TEST_F(HologresConnectionTest, SetOptionBytesReturnsNotImplemented) {
  uint8_t data[] = {1};
  EXPECT_EQ(conn_->SetOptionBytes("any", data, 1, &error_), ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

TEST_F(HologresConnectionTest, SetOptionDoubleReturnsNotImplemented) {
  EXPECT_EQ(conn_->SetOptionDouble("any", 1.0, &error_), ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

TEST_F(HologresConnectionTest, SetOptionIntReturnsNotImplemented) {
  EXPECT_EQ(conn_->SetOptionInt("any", 1, &error_), ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

TEST_F(HologresConnectionTest, SetOptionAutocommitDisabledNotImplemented) {
  EXPECT_EQ(
      conn_->SetOption(ADBC_CONNECTION_OPTION_AUTOCOMMIT, ADBC_OPTION_VALUE_DISABLED,
                       &error_),
      ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

TEST_F(HologresConnectionTest, SetOptionAutocommitEnabledOk) {
  EXPECT_EQ(
      conn_->SetOption(ADBC_CONNECTION_OPTION_AUTOCOMMIT, ADBC_OPTION_VALUE_ENABLED,
                       &error_),
      ADBC_STATUS_OK);
  TearDownError();
}

TEST_F(HologresConnectionTest, SetOptionUnknownKeyNotImplemented) {
  EXPECT_EQ(conn_->SetOption("unknown_option", "value", &error_),
            ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

TEST_F(HologresConnectionTest, InitNullDatabase) {
  EXPECT_EQ(conn_->Init(nullptr, &error_), ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

TEST_F(HologresConnectionTest, InitDatabaseWithNullPrivateData) {
  struct AdbcDatabase db;
  std::memset(&db, 0, sizeof(db));
  db.private_data = nullptr;
  EXPECT_EQ(conn_->Init(&db, &error_), ADBC_STATUS_INVALID_ARGUMENT);
  TearDownError();
}

// ---------------------------------------------------------------------------
// GetOption: autocommit (doesn't require PGconn)
// ---------------------------------------------------------------------------

TEST_F(HologresConnectionTest, GetOptionAutocommit) {
  char buf[64] = {};
  size_t length = sizeof(buf);
  EXPECT_EQ(conn_->GetOption(ADBC_CONNECTION_OPTION_AUTOCOMMIT, buf, &length, &error_),
            ADBC_STATUS_OK);
  EXPECT_STREQ(buf, ADBC_OPTION_VALUE_ENABLED);
  TearDownError();
}

TEST_F(HologresConnectionTest, GetOptionUnknown) {
  char buf[64] = {};
  size_t length = sizeof(buf);
  EXPECT_EQ(conn_->GetOption("unknown.option", buf, &length, &error_),
            ADBC_STATUS_NOT_FOUND);
  TearDownError();
}

// ---------------------------------------------------------------------------
// Release without Init (conn_ and cancel_ are null)
// ---------------------------------------------------------------------------

TEST_F(HologresConnectionTest, ReleaseWithoutInit) {
  EXPECT_EQ(conn_->Release(&error_), ADBC_STATUS_OK);
  TearDownError();
}

TEST_F(HologresConnectionTest, ReleaseMultipleTimes) {
  EXPECT_EQ(conn_->Release(&error_), ADBC_STATUS_OK);
  TearDownError();
  // Second release should also succeed (idempotent)
  EXPECT_EQ(conn_->Release(&error_), ADBC_STATUS_OK);
  TearDownError();
}

// ---------------------------------------------------------------------------
// SetOption: autocommit enabled + invalid
// ---------------------------------------------------------------------------

TEST_F(HologresConnectionTest, SetOptionAutocommitNonDisabledAccepted) {
  // Any value that is not "disabled" is accepted (Hologres is always autocommit)
  EXPECT_EQ(conn_->SetOption(ADBC_CONNECTION_OPTION_AUTOCOMMIT, "maybe", &error_),
            ADBC_STATUS_OK);
  TearDownError();
}

// ---------------------------------------------------------------------------
// GetStatisticNames (no DB required)
// ---------------------------------------------------------------------------

TEST_F(HologresConnectionTest, GetStatisticNames) {
  struct ArrowArrayStream stream;
  std::memset(&stream, 0, sizeof(stream));
  EXPECT_EQ(conn_->GetStatisticNames(&stream, &error_), ADBC_STATUS_OK);
  TearDownError();

  // Read the schema
  struct ArrowSchema schema;
  std::memset(&schema, 0, sizeof(schema));
  ASSERT_EQ(stream.get_schema(&stream, &schema), 0);

  // Should have 2 columns: statistic_name (string) and statistic_key (int16)
  EXPECT_EQ(schema.n_children, 2);
  EXPECT_STREQ(schema.children[0]->name, "statistic_name");
  EXPECT_STREQ(schema.children[0]->format, "u");
  EXPECT_STREQ(schema.children[1]->name, "statistic_key");
  EXPECT_STREQ(schema.children[1]->format, "s");

  // Read next batch — should be empty (no rows)
  struct ArrowArray array;
  std::memset(&array, 0, sizeof(array));
  ASSERT_EQ(stream.get_next(&stream, &array), 0);
  // Empty result (no statistics defined)
  if (array.release) {
    EXPECT_EQ(array.length, 0);
    array.release(&array);
  }

  if (schema.release) schema.release(&schema);
  stream.release(&stream);
}

// ---------------------------------------------------------------------------
// GetTableTypes (no DB required — uses static kPgTableTypes map)
// ---------------------------------------------------------------------------

TEST_F(HologresConnectionTest, GetTableTypes) {
  // GetTableTypes does not use the AdbcConnection* parameter internally;
  // it only reads the static kPgTableTypes map.  Pass a minimal struct.
  struct AdbcConnection adbc_conn;
  std::memset(&adbc_conn, 0, sizeof(adbc_conn));
  // Create a shared_ptr on the stack and point private_data to it
  // (mimics what HologresConnectionNew does).
  auto shared = std::shared_ptr<HologresConnection>(conn_.get(),
                                                    [](HologresConnection*) {});
  adbc_conn.private_data = &shared;

  struct ArrowArrayStream stream;
  std::memset(&stream, 0, sizeof(stream));
  EXPECT_EQ(conn_->GetTableTypes(&adbc_conn, &stream, &error_), ADBC_STATUS_OK);
  TearDownError();

  // Read schema
  struct ArrowSchema schema;
  std::memset(&schema, 0, sizeof(schema));
  ASSERT_EQ(stream.get_schema(&stream, &schema), 0);

  // Should have 1 column: table_type (string)
  ASSERT_GE(schema.n_children, 1);
  EXPECT_STREQ(schema.children[0]->name, "table_type");
  EXPECT_STREQ(schema.children[0]->format, "u");

  // Read rows — should contain the 6 table types from kPgTableTypes
  struct ArrowArray array;
  std::memset(&array, 0, sizeof(array));
  ASSERT_EQ(stream.get_next(&stream, &array), 0);
  ASSERT_NE(array.release, nullptr);

  // kPgTableTypes has 6 entries: table, view, materialized_view,
  // toast_table, foreign_table, partitioned_table
  EXPECT_EQ(array.length, 6);

  // Collect all values into a set
  auto* col = array.children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* data = reinterpret_cast<const char*>(col->buffers[2]);
  std::set<std::string> types;
  for (int64_t i = 0; i < array.length; i++) {
    types.insert(std::string(data + offsets[i], offsets[i + 1] - offsets[i]));
  }
  EXPECT_TRUE(types.count("table"));
  EXPECT_TRUE(types.count("view"));
  EXPECT_TRUE(types.count("materialized_view"));
  EXPECT_TRUE(types.count("toast_table"));
  EXPECT_TRUE(types.count("foreign_table"));
  EXPECT_TRUE(types.count("partitioned_table"));

  array.release(&array);
  if (schema.release) schema.release(&schema);
  stream.release(&stream);
}

}  // namespace adbchg
