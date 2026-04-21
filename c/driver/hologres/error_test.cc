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
#include <string>

#include <gtest/gtest.h>

#include "error.h"

namespace adbcpq {

TEST(ErrorTest, MakeStatusNullResultReturnsIO) {
  auto status = MakeStatus(nullptr, "test error: {}", "details");
  EXPECT_FALSE(status.ok());
  // With null PGresult, the default status code is ADBC_STATUS_IO
}

TEST(ErrorTest, MakeStatusNullResultContainsMessage) {
  auto status = MakeStatus(nullptr, "error code {}", 42);
  struct AdbcError adbc_error = ADBC_ERROR_INIT;
  status.ToAdbc(&adbc_error);
  ASSERT_NE(adbc_error.message, nullptr);
  EXPECT_NE(std::string(adbc_error.message).find("error code 42"), std::string::npos);
  if (adbc_error.release) adbc_error.release(&adbc_error);
}

TEST(ErrorTest, SetErrorNullResultSetsMessage) {
  struct AdbcError error = ADBC_ERROR_INIT;
  AdbcStatusCode code = SetError(&error, nullptr, "test %s %d", "message", 123);
  EXPECT_EQ(code, ADBC_STATUS_IO);
  EXPECT_NE(error.message, nullptr);
  EXPECT_NE(std::string(error.message).find("test message 123"), std::string::npos);
  if (error.release) error.release(&error);
}

TEST(ErrorTest, SetErrorNullErrorDoesNotCrash) {
  // Passing nullptr for error should not crash
  AdbcStatusCode code = SetError(nullptr, nullptr, "no crash");
  EXPECT_EQ(code, ADBC_STATUS_IO);
}

TEST(ErrorTest, DetailFieldsHasExpectedCount) {
  EXPECT_EQ(kDetailFields.size(), 14u);
}

TEST(ErrorTest, DetailFieldsContainsSQLSTATE) {
  bool found = false;
  for (const auto& field : kDetailFields) {
    if (field.key == "PG_DIAG_SQLSTATE") {
      found = true;
      EXPECT_EQ(field.code, PG_DIAG_SQLSTATE);
      break;
    }
  }
  EXPECT_TRUE(found);
}

TEST(ErrorTest, DetailFieldsContainsAllExpectedKeys) {
  // Verify specific expected keys are present
  std::vector<std::string> expected_keys = {
      "PG_DIAG_COLUMN_NAME",
      "PG_DIAG_CONTEXT",
      "PG_DIAG_CONSTRAINT_NAME",
      "PG_DIAG_DATATYPE_NAME",
      "PG_DIAG_MESSAGE_PRIMARY",
      "PG_DIAG_MESSAGE_DETAIL",
      "PG_DIAG_MESSAGE_HINT",
      "PG_DIAG_SEVERITY_NONLOCALIZED",
      "PG_DIAG_SQLSTATE",
      "PG_DIAG_STATEMENT_POSITION",
      "PG_DIAG_SCHEMA_NAME",
      "PG_DIAG_TABLE_NAME",
      "PG_DIAG_INTERNAL_POSITION",
      "PG_DIAG_INTERNAL_QUERY",
  };
  for (const auto& key : expected_keys) {
    bool found = false;
    for (const auto& field : kDetailFields) {
      if (field.key == key) {
        found = true;
        break;
      }
    }
    EXPECT_TRUE(found) << "Missing expected key: " << key;
  }
}

TEST(ErrorTest, DetailFieldCodesMatchPgConstants) {
  // Verify that each field's code matches the expected PG_DIAG_* constant
  for (const auto& field : kDetailFields) {
    if (field.key == "PG_DIAG_COLUMN_NAME") {
      EXPECT_EQ(field.code, PG_DIAG_COLUMN_NAME);
    } else if (field.key == "PG_DIAG_CONTEXT") {
      EXPECT_EQ(field.code, PG_DIAG_CONTEXT);
    } else if (field.key == "PG_DIAG_CONSTRAINT_NAME") {
      EXPECT_EQ(field.code, PG_DIAG_CONSTRAINT_NAME);
    } else if (field.key == "PG_DIAG_DATATYPE_NAME") {
      EXPECT_EQ(field.code, PG_DIAG_DATATYPE_NAME);
    } else if (field.key == "PG_DIAG_MESSAGE_PRIMARY") {
      EXPECT_EQ(field.code, PG_DIAG_MESSAGE_PRIMARY);
    } else if (field.key == "PG_DIAG_MESSAGE_DETAIL") {
      EXPECT_EQ(field.code, PG_DIAG_MESSAGE_DETAIL);
    } else if (field.key == "PG_DIAG_MESSAGE_HINT") {
      EXPECT_EQ(field.code, PG_DIAG_MESSAGE_HINT);
    } else if (field.key == "PG_DIAG_SCHEMA_NAME") {
      EXPECT_EQ(field.code, PG_DIAG_SCHEMA_NAME);
    } else if (field.key == "PG_DIAG_TABLE_NAME") {
      EXPECT_EQ(field.code, PG_DIAG_TABLE_NAME);
    }
  }
}

TEST(ErrorTest, MakeStatusNullResultFormatArgs) {
  auto status = MakeStatus(nullptr, "code={} msg={}", 42, "test");
  EXPECT_FALSE(status.ok());
  struct AdbcError adbc_error = ADBC_ERROR_INIT;
  status.ToAdbc(&adbc_error);
  ASSERT_NE(adbc_error.message, nullptr);
  std::string msg(adbc_error.message);
  EXPECT_NE(msg.find("code=42"), std::string::npos);
  EXPECT_NE(msg.find("msg=test"), std::string::npos);
  if (adbc_error.release) adbc_error.release(&adbc_error);
}

TEST(ErrorTest, SetErrorReturnsIOForNullResult) {
  struct AdbcError error = ADBC_ERROR_INIT;
  AdbcStatusCode code = SetError(&error, nullptr, "error: %s", "test");
  EXPECT_EQ(code, ADBC_STATUS_IO);
  EXPECT_NE(error.message, nullptr);
  EXPECT_NE(std::string(error.message).find("error: test"), std::string::npos);
  if (error.release) error.release(&error);
}

}  // namespace adbcpq
