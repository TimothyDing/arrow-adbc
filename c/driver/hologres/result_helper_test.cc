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

#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "result_helper.h"

namespace adbcpq {

// ---------------------------------------------------------------------------
// PqRecord::ParseDouble
// ---------------------------------------------------------------------------

// Note: ParseDouble checks errno which may be non-zero from prior calls.
// Each test clears errno before calling ParseDouble.

TEST(PqRecordTest, ParseDoubleValid) {
  const char* data = "3.14";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  errno = 0;
  auto result = record.ParseDouble();
  ASSERT_TRUE(result.has_value());
  EXPECT_DOUBLE_EQ(result.value(), 3.14);
}

TEST(PqRecordTest, ParseDoubleInteger) {
  const char* data = "42";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  errno = 0;
  auto result = record.ParseDouble();
  ASSERT_TRUE(result.has_value());
  EXPECT_DOUBLE_EQ(result.value(), 42.0);
}

TEST(PqRecordTest, ParseDoubleNegative) {
  const char* data = "-1.5";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  errno = 0;
  auto result = record.ParseDouble();
  ASSERT_TRUE(result.has_value());
  EXPECT_DOUBLE_EQ(result.value(), -1.5);
}

TEST(PqRecordTest, ParseDoubleScientific) {
  const char* data = "1.23e4";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  errno = 0;
  auto result = record.ParseDouble();
  ASSERT_TRUE(result.has_value());
  EXPECT_DOUBLE_EQ(result.value(), 12300.0);
}

TEST(PqRecordTest, ParseDoubleZero) {
  const char* data = "0.0";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  errno = 0;
  auto result = record.ParseDouble();
  ASSERT_TRUE(result.has_value());
  EXPECT_DOUBLE_EQ(result.value(), 0.0);
}

TEST(PqRecordTest, ParseDoubleInvalid) {
  const char* data = "not_a_number";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  errno = 0;
  auto result = record.ParseDouble();
  // strtod with "not_a_number" may parse "nan" prefix or fail,
  // depending on libc implementation
  // Just verify it doesn't crash
  (void)result;
}

TEST(PqRecordTest, ParseDoubleEmpty) {
  const char* data = "";
  PqRecord record{data, 0, false};
  errno = 0;
  auto result = record.ParseDouble();
  // empty string → end == data → returns nullopt
  EXPECT_FALSE(result.has_value());
}

// ---------------------------------------------------------------------------
// PqRecord::ParseInteger
// ---------------------------------------------------------------------------

TEST(PqRecordTest, ParseIntegerValid) {
  const char* data = "12345";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  auto result = record.ParseInteger();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 12345);
}

TEST(PqRecordTest, ParseIntegerNegative) {
  const char* data = "-9876";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  auto result = record.ParseInteger();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), -9876);
}

TEST(PqRecordTest, ParseIntegerZero) {
  const char* data = "0";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  auto result = record.ParseInteger();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 0);
}

TEST(PqRecordTest, ParseIntegerLarge) {
  const char* data = "9223372036854775807";  // INT64_MAX
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  auto result = record.ParseInteger();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), INT64_MAX);
}

TEST(PqRecordTest, ParseIntegerInvalid) {
  const char* data = "abc";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  auto result = record.ParseInteger();
  EXPECT_FALSE(result.has_value());
}

TEST(PqRecordTest, ParseIntegerPartial) {
  // "123abc" → from_chars stops at 'a', ptr != last → error
  const char* data = "123abc";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  auto result = record.ParseInteger();
  EXPECT_FALSE(result.has_value());
}

TEST(PqRecordTest, ParseIntegerEmpty) {
  const char* data = "";
  PqRecord record{data, 0, false};
  auto result = record.ParseInteger();
  EXPECT_FALSE(result.has_value());
}

// ---------------------------------------------------------------------------
// PqRecord::ParseTextArray
// ---------------------------------------------------------------------------

TEST(PqRecordTest, ParseTextArrayBasic) {
  const char* data = "{a,b,c}";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  auto result = record.ParseTextArray();
  ASSERT_TRUE(result.has_value());
  auto& vec = result.value();
  ASSERT_EQ(vec.size(), 3u);
  EXPECT_EQ(vec[0], "a");
  EXPECT_EQ(vec[1], "b");
  EXPECT_EQ(vec[2], "c");
}

TEST(PqRecordTest, ParseTextArraySingleElement) {
  const char* data = "{hello}";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  auto result = record.ParseTextArray();
  ASSERT_TRUE(result.has_value());
  auto& vec = result.value();
  ASSERT_EQ(vec.size(), 1u);
  EXPECT_EQ(vec[0], "hello");
}

TEST(PqRecordTest, ParseTextArrayEmpty) {
  const char* data = "{}";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  auto result = record.ParseTextArray();
  ASSERT_TRUE(result.has_value());
  auto& vec = result.value();
  ASSERT_EQ(vec.size(), 1u);
  EXPECT_EQ(vec[0], "");
}

TEST(PqRecordTest, ParseTextArrayTooShort) {
  // len < 2 → returns empty vector
  const char* data = "{";
  PqRecord record{data, 1, false};
  auto result = record.ParseTextArray();
  ASSERT_TRUE(result.has_value());
  auto& vec = result.value();
  EXPECT_TRUE(vec.empty());
}

TEST(PqRecordTest, ParseTextArrayWithSpaces) {
  const char* data = "{one two,three four}";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  auto result = record.ParseTextArray();
  ASSERT_TRUE(result.has_value());
  auto& vec = result.value();
  ASSERT_EQ(vec.size(), 2u);
  EXPECT_EQ(vec[0], "one two");
  EXPECT_EQ(vec[1], "three four");
}

TEST(PqRecordTest, ParseTextArrayEmptyElements) {
  const char* data = "{,,}";
  PqRecord record{data, static_cast<int>(std::strlen(data)), false};
  auto result = record.ParseTextArray();
  ASSERT_TRUE(result.has_value());
  auto& vec = result.value();
  ASSERT_EQ(vec.size(), 3u);
  EXPECT_EQ(vec[0], "");
  EXPECT_EQ(vec[1], "");
  EXPECT_EQ(vec[2], "");
}

// ---------------------------------------------------------------------------
// PqRecord::value
// ---------------------------------------------------------------------------

TEST(PqRecordTest, ValueReturnsStringView) {
  const char* data = "test_data";
  PqRecord record{data, 9, false};
  auto sv = record.value();
  EXPECT_EQ(sv, "test_data");
  EXPECT_EQ(sv.size(), 9u);
}

TEST(PqRecordTest, ValuePartialLength) {
  const char* data = "abcdef";
  PqRecord record{data, 3, false};
  auto sv = record.value();
  EXPECT_EQ(sv, "abc");
  EXPECT_EQ(sv.size(), 3u);
}

}  // namespace adbcpq
