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

#include <cmath>
#include <cstring>

#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.hpp>

#include "postgres_util.h"

namespace adbcpq {

// ---------------------------------------------------------------------------
// SwapNetworkToHost / SwapHostToNetwork round-trip tests
// ---------------------------------------------------------------------------

TEST(SwapByteOrderTest, UInt16RoundTrip) {
  uint16_t original = 0x1234;
  uint16_t swapped = SwapHostToNetwork(original);
  uint16_t restored = SwapNetworkToHost(swapped);
  EXPECT_EQ(original, restored);
}

TEST(SwapByteOrderTest, UInt32RoundTrip) {
  uint32_t original = 0x12345678;
  uint32_t swapped = SwapHostToNetwork(original);
  uint32_t restored = SwapNetworkToHost(swapped);
  EXPECT_EQ(original, restored);
}

TEST(SwapByteOrderTest, UInt64RoundTrip) {
  uint64_t original = 0x123456789ABCDEF0ULL;
  uint64_t swapped = SwapHostToNetwork(original);
  uint64_t restored = SwapNetworkToHost(swapped);
  EXPECT_EQ(original, restored);
}

TEST(SwapByteOrderTest, UInt16KnownValue) {
  // Big-endian 0x0100 should become 1 on little-endian (or 256 on big-endian)
  uint16_t val = 1;
  uint16_t network = SwapHostToNetwork(val);
  // The first byte (high byte) of network order should be 0, second byte 1
  uint8_t bytes[2];
  std::memcpy(bytes, &network, 2);
  EXPECT_EQ(bytes[0], 0);
  EXPECT_EQ(bytes[1], 1);
}

TEST(SwapByteOrderTest, UInt32KnownValue) {
  uint32_t val = 1;
  uint32_t network = SwapHostToNetwork(val);
  uint8_t bytes[4];
  std::memcpy(bytes, &network, 4);
  EXPECT_EQ(bytes[0], 0);
  EXPECT_EQ(bytes[1], 0);
  EXPECT_EQ(bytes[2], 0);
  EXPECT_EQ(bytes[3], 1);
}

TEST(SwapByteOrderTest, UInt64KnownValue) {
  uint64_t val = 1;
  uint64_t network = SwapHostToNetwork(val);
  uint8_t bytes[8];
  std::memcpy(bytes, &network, 8);
  for (int i = 0; i < 7; i++) {
    EXPECT_EQ(bytes[i], 0);
  }
  EXPECT_EQ(bytes[7], 1);
}

// ---------------------------------------------------------------------------
// LoadNetworkUInt / LoadNetworkInt tests
// ---------------------------------------------------------------------------

TEST(LoadNetworkTest, UInt16BigEndian) {
  // Big-endian representation of 0x1234
  const char buf[] = {0x12, 0x34};
  EXPECT_EQ(LoadNetworkUInt16(buf), 0x1234);
}

TEST(LoadNetworkTest, UInt32BigEndian) {
  const char buf[] = {0x12, 0x34, 0x56, 0x78};
  EXPECT_EQ(LoadNetworkUInt32(buf), 0x12345678u);
}

TEST(LoadNetworkTest, UInt64BigEndian) {
  const char buf[] = {0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x56, 0x78};
  EXPECT_EQ(static_cast<uint64_t>(LoadNetworkUInt64(buf)), 0x12345678ULL);
}

TEST(LoadNetworkTest, Int16Negative) {
  // -1 in big-endian int16: 0xFF 0xFF
  const char buf[] = {'\xFF', '\xFF'};
  EXPECT_EQ(LoadNetworkInt16(buf), -1);
}

TEST(LoadNetworkTest, Int32Negative) {
  // -1 in big-endian int32
  const char buf[] = {'\xFF', '\xFF', '\xFF', '\xFF'};
  EXPECT_EQ(LoadNetworkInt32(buf), -1);
}

TEST(LoadNetworkTest, Int64Negative) {
  const char buf[] = {'\xFF', '\xFF', '\xFF', '\xFF', '\xFF', '\xFF', '\xFF', '\xFF'};
  EXPECT_EQ(LoadNetworkInt64(buf), -1);
}

TEST(LoadNetworkTest, Float8KnownValue) {
  // IEEE 754 double 1.0 in big-endian: 3F F0 00 00 00 00 00 00
  const char buf[] = {0x3F, '\xF0', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  EXPECT_DOUBLE_EQ(LoadNetworkFloat8(buf), 1.0);
}

TEST(LoadNetworkTest, Float8NegativeValue) {
  // IEEE 754 double -1.0 in big-endian: BF F0 00 00 00 00 00 00
  const char buf[] = {'\xBF', '\xF0', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  EXPECT_DOUBLE_EQ(LoadNetworkFloat8(buf), -1.0);
}

// ---------------------------------------------------------------------------
// ToNetwork round-trip tests
// ---------------------------------------------------------------------------

TEST(ToNetworkTest, Int16RoundTrip) {
  int16_t original = -12345;
  uint16_t network = ToNetworkInt16(original);
  int16_t restored = LoadNetworkInt16(reinterpret_cast<const char*>(&network));
  EXPECT_EQ(original, restored);
}

TEST(ToNetworkTest, Int32RoundTrip) {
  int32_t original = -123456789;
  uint32_t network = ToNetworkInt32(original);
  int32_t restored = LoadNetworkInt32(reinterpret_cast<const char*>(&network));
  EXPECT_EQ(original, restored);
}

TEST(ToNetworkTest, Int64RoundTrip) {
  int64_t original = -1234567890123LL;
  uint64_t network = ToNetworkInt64(original);
  int64_t restored = LoadNetworkInt64(reinterpret_cast<const char*>(&network));
  EXPECT_EQ(original, restored);
}

TEST(ToNetworkTest, Float4RoundTrip) {
  float original = 3.14f;
  uint32_t network = ToNetworkFloat4(original);
  // Read it back as big-endian float
  uint32_t host = SwapNetworkToHost(network);
  float restored;
  std::memcpy(&restored, &host, sizeof(float));
  EXPECT_FLOAT_EQ(original, restored);
}

TEST(ToNetworkTest, Float8RoundTrip) {
  double original = 2.71828;
  uint64_t network = ToNetworkFloat8(original);
  uint64_t host = SwapNetworkToHost(network);
  double restored;
  std::memcpy(&restored, &host, sizeof(double));
  EXPECT_DOUBLE_EQ(original, restored);
}

// ---------------------------------------------------------------------------
// Handle RAII tests
// ---------------------------------------------------------------------------

TEST(HandleTest, ArrowSchemaZeroInitAndCleanup) {
  {
    Handle<struct ArrowSchema> handle;
    EXPECT_EQ(handle.value.release, nullptr);
    // Initialize it so we can verify cleanup
    ArrowSchemaInit(&handle.value);
    EXPECT_NE(handle.value.release, nullptr);
  }
  // Destructor should have called release - no leak
}

TEST(HandleTest, ArrowArrayZeroInitAndCleanup) {
  {
    Handle<struct ArrowArray> handle;
    EXPECT_EQ(handle.value.release, nullptr);
  }
}

TEST(HandleTest, ArrowBufferResetCalled) {
  {
    Handle<struct ArrowBuffer> handle;
    ArrowBufferInit(&handle.value);
    ArrowBufferAppendInt8(&handle.value, 42);
    EXPECT_GT(handle.value.size_bytes, 0);
  }
  // Destructor should reset the buffer
}

TEST(HandleTest, ResetExplicit) {
  Handle<struct ArrowSchema> handle;
  ArrowSchemaInit(&handle.value);
  EXPECT_NE(handle.value.release, nullptr);
  handle.reset();
  // After reset, release should have been called
}

// ---------------------------------------------------------------------------
// PqEscapedString
// ---------------------------------------------------------------------------

TEST(PqEscapedStringTest, DefaultConstructorIsNull) {
  PqEscapedString s;
  EXPECT_FALSE(static_cast<bool>(s));
  EXPECT_EQ(s.c_str(), nullptr);
}

TEST(PqEscapedStringTest, ExplicitConstructorWithNull) {
  PqEscapedString s(nullptr);
  EXPECT_FALSE(static_cast<bool>(s));
  EXPECT_EQ(s.c_str(), nullptr);
}

TEST(PqEscapedStringTest, MoveConstructor) {
  // Use strdup to simulate a PQescapeIdentifier-allocated string
  // (strdup uses malloc, and PQfreemem calls free on macOS/Linux)
  char* raw = strdup("\"test\"");
  PqEscapedString a(raw);
  EXPECT_TRUE(static_cast<bool>(a));
  EXPECT_STREQ(a.c_str(), "\"test\"");

  PqEscapedString b(std::move(a));
  EXPECT_TRUE(static_cast<bool>(b));
  EXPECT_STREQ(b.c_str(), "\"test\"");
  // a should be null after move
  EXPECT_FALSE(static_cast<bool>(a));
  EXPECT_EQ(a.c_str(), nullptr);
}

TEST(PqEscapedStringTest, MoveAssignment) {
  char* raw1 = strdup("first");
  char* raw2 = strdup("second");
  PqEscapedString a(raw1);
  PqEscapedString b(raw2);

  b = std::move(a);
  EXPECT_STREQ(b.c_str(), "first");
  EXPECT_FALSE(static_cast<bool>(a));
}

TEST(PqEscapedStringTest, MoveAssignmentSelf) {
  char* raw = strdup("self");
  PqEscapedString a(raw);
  // Suppress -Wself-move: we intentionally test self-move safety
  PqEscapedString& ref = a;
  a = std::move(ref);
  // Self-move should be safe (no-op due to this != &other check)
  EXPECT_TRUE(static_cast<bool>(a));
  EXPECT_STREQ(a.c_str(), "self");
}

// ---------------------------------------------------------------------------
// Float special values
// ---------------------------------------------------------------------------

TEST(LoadNetworkTest, Float8NaN) {
  double nan_val = std::numeric_limits<double>::quiet_NaN();
  uint64_t network = ToNetworkFloat8(nan_val);
  EXPECT_TRUE(std::isnan(LoadNetworkFloat8(reinterpret_cast<const char*>(&network))));
}

TEST(LoadNetworkTest, Float8Infinity) {
  double inf_val = std::numeric_limits<double>::infinity();
  uint64_t network = ToNetworkFloat8(inf_val);
  EXPECT_EQ(LoadNetworkFloat8(reinterpret_cast<const char*>(&network)), inf_val);
}

TEST(LoadNetworkTest, Float8NegativeInfinity) {
  double neg_inf = -std::numeric_limits<double>::infinity();
  uint64_t network = ToNetworkFloat8(neg_inf);
  EXPECT_EQ(LoadNetworkFloat8(reinterpret_cast<const char*>(&network)), neg_inf);
}

TEST(LoadNetworkTest, Float4NaN) {
  float nan_val = std::numeric_limits<float>::quiet_NaN();
  uint32_t network = ToNetworkFloat4(nan_val);
  uint32_t host = SwapNetworkToHost(network);
  float result;
  std::memcpy(&result, &host, sizeof(result));
  EXPECT_TRUE(std::isnan(result));
}

// ---------------------------------------------------------------------------
// Handle: ArrowArrayView specialization
// ---------------------------------------------------------------------------

TEST(HandleTest, ArrowArrayViewReleaser) {
  Handle<struct ArrowArrayView> handle;
  // Default state: storage_type is NANOARROW_TYPE_UNINITIALIZED
  // Reset should be safe (no-op)
  handle.reset();

  // Initialize with a type
  ArrowArrayViewInitFromType(&handle.value, NANOARROW_TYPE_INT32);
  EXPECT_NE(handle.value.storage_type, NANOARROW_TYPE_UNINITIALIZED);
  // Reset should call ArrowArrayViewReset
  handle.reset();
}

TEST(HandleTest, DoubleReset) {
  Handle<struct ArrowSchema> handle;
  ArrowSchemaInit(&handle.value);
  EXPECT_NE(handle.value.release, nullptr);
  handle.reset();
  // Second reset should be safe
  handle.reset();
}

}  // namespace adbcpq
