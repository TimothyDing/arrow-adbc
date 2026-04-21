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

}  // namespace adbchg
