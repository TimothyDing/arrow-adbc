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

}  // namespace adbcpq
