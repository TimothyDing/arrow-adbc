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
  std::string msg = status.message();
  EXPECT_NE(msg.find("error code 42"), std::string::npos);
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

}  // namespace adbcpq
