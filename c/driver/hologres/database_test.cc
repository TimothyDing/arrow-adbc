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

#include <array>
#include <cstring>
#include <string>

#include <gtest/gtest.h>
#include <arrow-adbc/adbc.h>

#include "database.h"

namespace adbchg {

// ===========================================================================
// ParsePrefixedVersion tests
// ===========================================================================

TEST(ParsePrefixedVersionTest, HologresFormat) {
  auto v = ParsePrefixedVersion(
      "Hologres 4.1.12 (tag: release-20240101), compatible with PostgreSQL 11.3",
      "Hologres");
  EXPECT_EQ(v[0], 4);
  EXPECT_EQ(v[1], 1);
  EXPECT_EQ(v[2], 12);
}

TEST(ParsePrefixedVersionTest, PostgreSQLFormat) {
  auto v = ParsePrefixedVersion(
      "PostgreSQL 11.3 on x86_64-pc-linux-gnu, compiled by gcc 7.3.0", "PostgreSQL");
  EXPECT_EQ(v[0], 11);
  EXPECT_EQ(v[1], 3);
  EXPECT_EQ(v[2], 0);
}

TEST(ParsePrefixedVersionTest, PrefixNotFound) {
  auto v = ParsePrefixedVersion("MySQL 8.0.26", "Hologres");
  EXPECT_EQ(v[0], 0);
  EXPECT_EQ(v[1], 0);
  EXPECT_EQ(v[2], 0);
}

TEST(ParsePrefixedVersionTest, EmptyString) {
  auto v = ParsePrefixedVersion("", "Hologres");
  EXPECT_EQ(v[0], 0);
  EXPECT_EQ(v[1], 0);
  EXPECT_EQ(v[2], 0);
}

TEST(ParsePrefixedVersionTest, PrefixOnly) {
  auto v = ParsePrefixedVersion("Hologres", "Hologres");
  EXPECT_EQ(v[0], 0);
  EXPECT_EQ(v[1], 0);
  EXPECT_EQ(v[2], 0);
}

TEST(ParsePrefixedVersionTest, PrefixWithTrailingSpacesOnly) {
  auto v = ParsePrefixedVersion("Hologres   ", "Hologres");
  EXPECT_EQ(v[0], 0);
  EXPECT_EQ(v[1], 0);
  EXPECT_EQ(v[2], 0);
}

TEST(ParsePrefixedVersionTest, TwoComponents) {
  auto v = ParsePrefixedVersion("Hologres 4.1", "Hologres");
  EXPECT_EQ(v[0], 4);
  EXPECT_EQ(v[1], 1);
  EXPECT_EQ(v[2], 0);
}

TEST(ParsePrefixedVersionTest, SingleComponent) {
  auto v = ParsePrefixedVersion("Hologres 4", "Hologres");
  EXPECT_EQ(v[0], 4);
  EXPECT_EQ(v[1], 0);
  EXPECT_EQ(v[2], 0);
}

TEST(ParsePrefixedVersionTest, FourComponents) {
  // Only first 3 components should be parsed
  auto v = ParsePrefixedVersion("Hologres 4.1.12.3", "Hologres");
  EXPECT_EQ(v[0], 4);
  EXPECT_EQ(v[1], 1);
  EXPECT_EQ(v[2], 12);
}

TEST(ParsePrefixedVersionTest, DashSeparator) {
  auto v = ParsePrefixedVersion("Hologres 4-1-12", "Hologres");
  EXPECT_EQ(v[0], 4);
  EXPECT_EQ(v[1], 1);
  EXPECT_EQ(v[2], 12);
}

TEST(ParsePrefixedVersionTest, MixedSeparators) {
  auto v = ParsePrefixedVersion("Hologres 4.1-12", "Hologres");
  EXPECT_EQ(v[0], 4);
  EXPECT_EQ(v[1], 1);
  EXPECT_EQ(v[2], 12);
}

TEST(ParsePrefixedVersionTest, TrailingJunk) {
  auto v = ParsePrefixedVersion("Hologres 4.1.12rc1", "Hologres");
  EXPECT_EQ(v[0], 4);
  EXPECT_EQ(v[1], 1);
  // std::from_chars stops at 'r', so it parses 12
  EXPECT_EQ(v[2], 12);
}

TEST(ParsePrefixedVersionTest, ExtractBothVersions) {
  std::string_view full =
      "Hologres 4.1.12 (tag: release), compatible with PostgreSQL 11.3";
  auto hg = ParsePrefixedVersion(full, "Hologres");
  auto pg = ParsePrefixedVersion(full, "PostgreSQL");
  EXPECT_EQ(hg[0], 4);
  EXPECT_EQ(hg[1], 1);
  EXPECT_EQ(hg[2], 12);
  EXPECT_EQ(pg[0], 11);
  EXPECT_EQ(pg[1], 3);
  EXPECT_EQ(pg[2], 0);
}

// ===========================================================================
// MakeFixedFeUri tests
// ===========================================================================

TEST(MakeFixedFeUriTest, NoQueryParams) {
  std::string result = MakeFixedFeUri("postgres://host/db");
  EXPECT_EQ(result, "postgres://host/db?options=type%3Dfixed");
}

TEST(MakeFixedFeUriTest, ExistingQueryParams) {
  std::string result = MakeFixedFeUri("postgres://host/db?sslmode=require");
  EXPECT_EQ(result, "postgres://host/db?sslmode=require&options=type%3Dfixed");
}

TEST(MakeFixedFeUriTest, ExistingOptionsParam) {
  std::string result = MakeFixedFeUri("postgres://host/db?options=foo");
  EXPECT_EQ(result, "postgres://host/db?options=type%3Dfixed%20foo");
}

TEST(MakeFixedFeUriTest, ExistingOptionsWithOtherParams) {
  std::string result =
      MakeFixedFeUri("postgres://host/db?sslmode=require&options=foo&bar=baz");
  EXPECT_EQ(result,
            "postgres://host/db?sslmode=require&options=type%3Dfixed%20foo&bar=baz");
}

TEST(MakeFixedFeUriTest, EmptyUri) {
  std::string result = MakeFixedFeUri("");
  EXPECT_EQ(result, "?options=type%3Dfixed");
}

// ===========================================================================
// HologresDatabase option tests
// ===========================================================================

class HologresDatabaseTest : public ::testing::Test {
 protected:
  void SetUp() override { db_ = std::make_unique<HologresDatabase>(); }

  std::unique_ptr<HologresDatabase> db_;
  struct AdbcError error_ = ADBC_ERROR_INIT;

  void TearDownError() {
    if (error_.release) error_.release(&error_);
  }
};

TEST_F(HologresDatabaseTest, SetOptionUri) {
  EXPECT_EQ(db_->SetOption(ADBC_OPTION_URI, "postgres://localhost/test", &error_),
            ADBC_STATUS_OK);
  EXPECT_EQ(db_->uri(), "postgres://localhost/test");
  TearDownError();
}

TEST_F(HologresDatabaseTest, SetOptionUnknownKey) {
  EXPECT_EQ(db_->SetOption("unknown_key", "value", &error_), ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

TEST_F(HologresDatabaseTest, GetOptionUri) {
  db_->SetOption(ADBC_OPTION_URI, "test_uri", &error_);
  char buf[64] = {};
  size_t length = sizeof(buf);
  EXPECT_EQ(db_->GetOption(ADBC_OPTION_URI, buf, &length, &error_), ADBC_STATUS_OK);
  EXPECT_STREQ(buf, "test_uri");
  EXPECT_EQ(length, 9u);  // "test_uri" + null terminator
  TearDownError();
}

TEST_F(HologresDatabaseTest, GetOptionUriBufferTooSmall) {
  db_->SetOption(ADBC_OPTION_URI, "test_uri", &error_);
  char buf[4] = {};
  size_t length = sizeof(buf);
  EXPECT_EQ(db_->GetOption(ADBC_OPTION_URI, buf, &length, &error_), ADBC_STATUS_OK);
  // Length should still report the required size
  EXPECT_EQ(length, 9u);
  TearDownError();
}

TEST_F(HologresDatabaseTest, GetOptionUnknownKey) {
  char buf[64] = {};
  size_t length = sizeof(buf);
  EXPECT_EQ(db_->GetOption("unknown", buf, &length, &error_), ADBC_STATUS_NOT_FOUND);
  TearDownError();
}

TEST_F(HologresDatabaseTest, GetOptionBytesReturnsNotFound) {
  uint8_t buf[64] = {};
  size_t length = sizeof(buf);
  EXPECT_EQ(db_->GetOptionBytes("any", buf, &length, &error_), ADBC_STATUS_NOT_FOUND);
  TearDownError();
}

TEST_F(HologresDatabaseTest, GetOptionIntReturnsNotFound) {
  int64_t val;
  EXPECT_EQ(db_->GetOptionInt("any", &val, &error_), ADBC_STATUS_NOT_FOUND);
  TearDownError();
}

TEST_F(HologresDatabaseTest, GetOptionDoubleReturnsNotFound) {
  double val;
  EXPECT_EQ(db_->GetOptionDouble("any", &val, &error_), ADBC_STATUS_NOT_FOUND);
  TearDownError();
}

TEST_F(HologresDatabaseTest, SetOptionBytesReturnsNotImpl) {
  uint8_t data[] = {1, 2, 3};
  EXPECT_EQ(db_->SetOptionBytes("any", data, 3, &error_), ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

TEST_F(HologresDatabaseTest, SetOptionIntReturnsNotImpl) {
  EXPECT_EQ(db_->SetOptionInt("any", 42, &error_), ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

TEST_F(HologresDatabaseTest, SetOptionDoubleReturnsNotImpl) {
  EXPECT_EQ(db_->SetOptionDouble("any", 3.14, &error_), ADBC_STATUS_NOT_IMPLEMENTED);
  TearDownError();
}

TEST_F(HologresDatabaseTest, ReleaseNoOpenConnections) {
  EXPECT_EQ(db_->Release(&error_), ADBC_STATUS_OK);
  TearDownError();
}

TEST_F(HologresDatabaseTest, InitEmptyUri) {
  EXPECT_EQ(db_->Init(&error_), ADBC_STATUS_INVALID_STATE);
  TearDownError();
}

TEST_F(HologresDatabaseTest, ConstructorDefaults) {
  EXPECT_TRUE(db_->uri().empty());
  EXPECT_NE(db_->type_resolver(), nullptr);
  EXPECT_EQ(db_->VendorName(), "Hologres");
  EXPECT_EQ(db_->HologresVersion()[0], 0);
  EXPECT_EQ(db_->PostgresVersion()[0], 0);
}

}  // namespace adbchg
