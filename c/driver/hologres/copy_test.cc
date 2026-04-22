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
#include <nanoarrow/nanoarrow.hpp>

#include "copy/reader.h"
#include "postgres_type.h"
#include "test_util.h"

// Pre-generated PG COPY binary data from the PostgreSQL driver tests
#include "postgresql/copy/postgres_copy_test_common.h"

namespace adbcpq {

// CopyReaderTester and MakeRecordType are provided by test_util.h

// ---------------------------------------------------------------------------
// ReadHeader
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadHeaderValidSignature) {
  PostgresType record = MakeRecordType(PostgresTypeId::kBool);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);
  ASSERT_EQ(reader.InitFieldReaders(nullptr), NANOARROW_OK);

  ArrowBufferView view;
  view.data.as_uint8 = kTestPgCopyBoolean;
  view.size_bytes = sizeof(kTestPgCopyBoolean);

  ArrowError error;
  EXPECT_EQ(reader.ReadHeader(&view, &error), NANOARROW_OK);
}

TEST(CopyReaderTest, ReadHeaderInvalidSignature) {
  PostgresType record = MakeRecordType(PostgresTypeId::kBool);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);
  ASSERT_EQ(reader.InitFieldReaders(nullptr), NANOARROW_OK);

  uint8_t bad_data[] = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
                        0x08, 0x09, 0x0A, 0x0B, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00};
  ArrowBufferView view;
  view.data.as_uint8 = bad_data;
  view.size_bytes = sizeof(bad_data);

  ArrowError error;
  EXPECT_EQ(reader.ReadHeader(&view, &error), EINVAL);
}

TEST(CopyReaderTest, ReadHeaderTooShort) {
  PostgresType record = MakeRecordType(PostgresTypeId::kBool);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);
  ASSERT_EQ(reader.InitFieldReaders(nullptr), NANOARROW_OK);

  uint8_t tiny[] = {0x50, 0x47};
  ArrowBufferView view;
  view.data.as_uint8 = tiny;
  view.size_bytes = sizeof(tiny);

  ArrowError error;
  EXPECT_EQ(reader.ReadHeader(&view, &error), EINVAL);
}

// ---------------------------------------------------------------------------
// Read Boolean
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadBoolean) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kBool));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyBoolean, sizeof(kTestPgCopyBoolean)), NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto* col = array->children[0];
  EXPECT_EQ(col->length, 3);
  // First value: TRUE, Second: FALSE, Third: NULL
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read SmallInt
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadSmallInt) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kInt2));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopySmallInt, sizeof(kTestPgCopySmallInt)),
            NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 5);
  ASSERT_EQ(array->n_children, 1);

  auto* col = array->children[0];
  EXPECT_EQ(col->length, 5);
  auto* data = reinterpret_cast<const int16_t*>(col->buffers[1]);
  EXPECT_EQ(data[0], -123);
  EXPECT_EQ(data[1], -1);
  EXPECT_EQ(data[2], 1);
  EXPECT_EQ(data[3], 123);
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read Integer
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadInteger) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kInt4));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyInteger, sizeof(kTestPgCopyInteger)), NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 5);

  auto* col = array->children[0];
  auto* data = reinterpret_cast<const int32_t*>(col->buffers[1]);
  EXPECT_EQ(data[0], -123);
  EXPECT_EQ(data[1], -1);
  EXPECT_EQ(data[2], 1);
  EXPECT_EQ(data[3], 123);
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read BigInt
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadBigInt) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kInt8));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyBigInt, sizeof(kTestPgCopyBigInt)), NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 5);

  auto* col = array->children[0];
  auto* data = reinterpret_cast<const int64_t*>(col->buffers[1]);
  EXPECT_EQ(data[0], -123);
  EXPECT_EQ(data[1], -1);
  EXPECT_EQ(data[2], 1);
  EXPECT_EQ(data[3], 123);
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read Real (float4)
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadReal) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kFloat4));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyReal, sizeof(kTestPgCopyReal)), NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 5);

  auto* col = array->children[0];
  auto* data = reinterpret_cast<const float*>(col->buffers[1]);
  EXPECT_FLOAT_EQ(data[0], -123.456f);
  EXPECT_FLOAT_EQ(data[1], -1.0f);
  EXPECT_FLOAT_EQ(data[2], 1.0f);
  EXPECT_FLOAT_EQ(data[3], 123.456f);
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read Double Precision (float8)
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadDoublePrecision) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kFloat8));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(
      tester.ReadAll(kTestPgCopyDoublePrecision, sizeof(kTestPgCopyDoublePrecision)),
      NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 5);

  auto* col = array->children[0];
  auto* data = reinterpret_cast<const double*>(col->buffers[1]);
  EXPECT_DOUBLE_EQ(data[0], -123.456);
  EXPECT_DOUBLE_EQ(data[1], -1.0);
  EXPECT_DOUBLE_EQ(data[2], 1.0);
  EXPECT_DOUBLE_EQ(data[3], 123.456);
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read Text
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadText) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kText));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyText, sizeof(kTestPgCopyText)), NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_GE(array->length, 1);
  ASSERT_EQ(array->n_children, 1);
}

// ---------------------------------------------------------------------------
// Read Binary
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadBinary) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kBytea));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyBinary, sizeof(kTestPgCopyBinary)), NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_GE(array->length, 1);
}

// ---------------------------------------------------------------------------
// Schema inference
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, InferOutputSchemaBoolean) {
  PostgresType record = MakeRecordType(PostgresTypeId::kBool);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->format, "+s");
  EXPECT_EQ(schema->n_children, 1);
  EXPECT_STREQ(schema->children[0]->format, "b");
}

TEST(CopyReaderTest, InferOutputSchemaInt4) {
  PostgresType record = MakeRecordType(PostgresTypeId::kInt4);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->children[0]->format, "i");
}

TEST(CopyReaderTest, InferOutputSchemaFloat8) {
  PostgresType record = MakeRecordType(PostgresTypeId::kFloat8);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->children[0]->format, "g");
}

TEST(CopyReaderTest, InferOutputSchemaText) {
  PostgresType record = MakeRecordType(PostgresTypeId::kText);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->children[0]->format, "u");
}

TEST(CopyReaderTest, InferOutputSchemaTimestamp) {
  PostgresType record = MakeRecordType(PostgresTypeId::kTimestamp);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->children[0]->format, "tsu:");
}

TEST(CopyReaderTest, InferOutputSchemaTimestamptz) {
  PostgresType record = MakeRecordType(PostgresTypeId::kTimestamptz);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->children[0]->format, "tsu:UTC");
}

// ---------------------------------------------------------------------------
// Init validation
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, InitWithNonRecordType) {
  PostgresType non_record(PostgresTypeId::kBool);
  PostgresCopyStreamReader reader;
  EXPECT_EQ(reader.Init(non_record), EINVAL);
}

TEST(CopyReaderTest, InitFieldReadersWithoutSchema) {
  PostgresType record = MakeRecordType(PostgresTypeId::kBool);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  // Calling InitFieldReaders without InferOutputSchema should fail
  // because schema_->release is nullptr
  EXPECT_EQ(reader.InitFieldReaders(nullptr), EINVAL);
}

// ---------------------------------------------------------------------------
// MakeCopyFieldReader factory
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, MakeFieldReaderBool) {
  PostgresType pg_type(PostgresTypeId::kBool);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_BOOL);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
  EXPECT_NE(reader, nullptr);
}

TEST(CopyReaderTest, MakeFieldReaderInt16) {
  PostgresType pg_type(PostgresTypeId::kInt2);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INT16);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
}

TEST(CopyReaderTest, MakeFieldReaderInt32) {
  PostgresType pg_type(PostgresTypeId::kInt4);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INT32);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
}

TEST(CopyReaderTest, MakeFieldReaderInt64) {
  PostgresType pg_type(PostgresTypeId::kInt8);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INT64);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
}

TEST(CopyReaderTest, MakeFieldReaderFloat) {
  PostgresType pg_type(PostgresTypeId::kFloat4);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_FLOAT);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
}

TEST(CopyReaderTest, MakeFieldReaderDouble) {
  PostgresType pg_type(PostgresTypeId::kFloat8);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_DOUBLE);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
}

TEST(CopyReaderTest, MakeFieldReaderString) {
  PostgresType pg_type(PostgresTypeId::kText);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_STRING);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
}

TEST(CopyReaderTest, MakeFieldReaderBinary) {
  PostgresType pg_type(PostgresTypeId::kBytea);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_BINARY);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
}

TEST(CopyReaderTest, MakeFieldReaderUnsupportedConversion) {
  // Trying to read a bool type as int16 should fail
  PostgresType pg_type(PostgresTypeId::kBool);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INT16);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), EINVAL);
}

// ---------------------------------------------------------------------------
// Read Date
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadDate) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kDate));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyDate, sizeof(kTestPgCopyDate)), NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto* col = array->children[0];
  EXPECT_EQ(col->length, 3);
  auto* data = reinterpret_cast<const int32_t*>(col->buffers[1]);
  // 1900-01-01: PG=-36524, Arrow=-36524+10957=-25567
  EXPECT_EQ(data[0], -25567);
  // 2100-01-01: PG=36525, Arrow=36525+10957=47482
  EXPECT_EQ(data[1], 47482);
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read Time
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadTime) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kTime));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyTime, sizeof(kTestPgCopyTime)), NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 4);
  ASSERT_EQ(array->n_children, 1);

  auto* col = array->children[0];
  EXPECT_EQ(col->length, 4);
  auto* data = reinterpret_cast<const int64_t*>(col->buffers[1]);
  // 00:00:00 → 0 µs
  EXPECT_EQ(data[0], 0);
  // 23:59:59 → 86399000000 µs
  EXPECT_EQ(data[1], 86399000000LL);
  // 13:42:56.123456 → 49376123456 µs
  EXPECT_EQ(data[2], 49376123456LL);
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read Timestamp
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadTimestamp) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kTimestamp));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyTimestamp, sizeof(kTestPgCopyTimestamp)),
            NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto* col = array->children[0];
  EXPECT_EQ(col->length, 3);
  // Values have kPostgresTimestampEpoch (946684800000000) added
  auto* data = reinterpret_cast<const int64_t*>(col->buffers[1]);
  // 1900-01-01 12:34:56 and 2100-01-01 12:34:56 - both non-null
  EXPECT_NE(data[0], 0);
  EXPECT_NE(data[1], 0);
  EXPECT_GT(data[1], data[0]);  // 2100 > 1900
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read Interval
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadInterval) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kInterval));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyInterval, sizeof(kTestPgCopyInterval)),
            NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto* col = array->children[0];
  EXPECT_EQ(col->length, 3);
  // Arrow interval_month_day_nano: {months(i32), days(i32), time_ns(i64)}
  auto* buf = reinterpret_cast<const uint8_t*>(col->buffers[1]);

  // Row 0: -1 months, -2 days, -4 seconds = -4000000 µs = -4000000000 ns
  int32_t months0, days0;
  int64_t ns0;
  std::memcpy(&months0, buf + 0, 4);
  std::memcpy(&days0, buf + 4, 4);
  std::memcpy(&ns0, buf + 8, 8);
  EXPECT_EQ(months0, -1);
  EXPECT_EQ(days0, -2);
  EXPECT_EQ(ns0, -4000000000LL);

  // Row 1: 1 months, 2 days, 4 seconds = 4000000 µs = 4000000000 ns
  int32_t months1, days1;
  int64_t ns1;
  std::memcpy(&months1, buf + 16, 4);
  std::memcpy(&days1, buf + 20, 4);
  std::memcpy(&ns1, buf + 24, 8);
  EXPECT_EQ(months1, 1);
  EXPECT_EQ(days1, 2);
  EXPECT_EQ(ns1, 4000000000LL);

  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read Numeric (output as string)
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadNumeric) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kNumeric));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyNumeric, sizeof(kTestPgCopyNumeric)), NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 9);
  ASSERT_EQ(array->n_children, 1);

  auto* col = array->children[0];
  EXPECT_EQ(col->length, 9);
  // Numeric is read as STRING; verify via offsets + data buffers
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);

  auto get_str = [&](int64_t i) -> std::string {
    return std::string(str_data + offsets[i], offsets[i + 1] - offsets[i]);
  };

  EXPECT_EQ(get_str(0), "1000000");
  EXPECT_EQ(get_str(1), "0.00001234");
  EXPECT_EQ(get_str(2), "1.0000");
  EXPECT_EQ(get_str(3), "-123.456");
  EXPECT_EQ(get_str(4), "123.456");
  EXPECT_EQ(get_str(5), "nan");
  EXPECT_EQ(get_str(6), "-inf");
  EXPECT_EQ(get_str(7), "inf");
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read JSONB
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadJsonb) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kJsonb));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyJsonb, sizeof(kTestPgCopyJsonb)), NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto* col = array->children[0];
  EXPECT_EQ(col->length, 3);
  // JSONB reader strips the 0x01 version prefix
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);

  auto get_str = [&](int64_t i) -> std::string {
    return std::string(str_data + offsets[i], offsets[i + 1] - offsets[i]);
  };

  EXPECT_EQ(get_str(0), "[1, 2, 3]");
  EXPECT_EQ(get_str(1), "[4, 5, 6]");
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read JSON (plain, no version prefix)
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadJson) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kJson));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyJson, sizeof(kTestPgCopyJson)), NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto* col = array->children[0];
  EXPECT_EQ(col->length, 3);
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);

  auto get_str = [&](int64_t i) -> std::string {
    return std::string(str_data + offsets[i], offsets[i + 1] - offsets[i]);
  };

  EXPECT_EQ(get_str(0), "[1, 2, 3]");
  EXPECT_EQ(get_str(1), "[4, 5, 6]");
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read Enum (as string)
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadEnum) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kEnum));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyEnum, sizeof(kTestPgCopyEnum)), NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto* col = array->children[0];
  EXPECT_EQ(col->length, 3);
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);

  auto get_str = [&](int64_t i) -> std::string {
    return std::string(str_data + offsets[i], offsets[i + 1] - offsets[i]);
  };

  EXPECT_EQ(get_str(0), "ok");
  EXPECT_EQ(get_str(1), "sad");
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read Integer Array
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadIntegerArray) {
  // Array<Int4> column
  PostgresType array_type = PostgresType(PostgresTypeId::kInt4).Array(0, "_int4");
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", array_type);

  CopyReaderTester tester(record);
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyIntegerArray, sizeof(kTestPgCopyIntegerArray)),
            NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto* list_col = array->children[0];
  EXPECT_EQ(list_col->length, 3);
  EXPECT_EQ(list_col->null_count, 1);

  // Check the child values: {-123, -1} and {0, 1, 123}
  auto* values = list_col->children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(list_col->buffers[1]);
  auto* int_data = reinterpret_cast<const int32_t*>(values->buffers[1]);

  // First list: {-123, -1}
  EXPECT_EQ(offsets[0], 0);
  EXPECT_EQ(offsets[1], 2);
  EXPECT_EQ(int_data[0], -123);
  EXPECT_EQ(int_data[1], -1);

  // Second list: {0, 1, 123}
  EXPECT_EQ(offsets[2], 5);
  EXPECT_EQ(int_data[2], 0);
  EXPECT_EQ(int_data[3], 1);
  EXPECT_EQ(int_data[4], 123);
}

// ---------------------------------------------------------------------------
// Read Custom Record
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadCustomRecord) {
  // Record with (int4, float8)
  PostgresType inner_record(PostgresTypeId::kRecord);
  inner_record.AppendChild("nested1", PostgresType(PostgresTypeId::kInt4));
  inner_record.AppendChild("nested2", PostgresType(PostgresTypeId::kFloat8));

  PostgresType outer_record(PostgresTypeId::kRecord);
  outer_record.AppendChild("col", inner_record);

  CopyReaderTester tester(outer_record);
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyCustomRecord, sizeof(kTestPgCopyCustomRecord)),
            NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto* struct_col = array->children[0];
  EXPECT_EQ(struct_col->length, 3);
  EXPECT_EQ(struct_col->null_count, 1);

  // Check int4 child
  auto* int_child = struct_col->children[0];
  auto* int_data = reinterpret_cast<const int32_t*>(int_child->buffers[1]);
  EXPECT_EQ(int_data[0], 123);
  EXPECT_EQ(int_data[1], 12);

  // Check float8 child
  auto* dbl_child = struct_col->children[1];
  auto* dbl_data = reinterpret_cast<const double*>(dbl_child->buffers[1]);
  EXPECT_DOUBLE_EQ(dbl_data[0], 456.789);
  EXPECT_DOUBLE_EQ(dbl_data[1], 345.678);
}

// ---------------------------------------------------------------------------
// Schema inference - additional types
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, InferOutputSchemaDate) {
  PostgresType record = MakeRecordType(PostgresTypeId::kDate);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->children[0]->format, "tdD");
}

TEST(CopyReaderTest, InferOutputSchemaTime) {
  PostgresType record = MakeRecordType(PostgresTypeId::kTime);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  // Time64 microsecond: "ttu"
  EXPECT_STREQ(schema->children[0]->format, "ttu");
}

TEST(CopyReaderTest, InferOutputSchemaInterval) {
  PostgresType record = MakeRecordType(PostgresTypeId::kInterval);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->children[0]->format, "tin");
}

TEST(CopyReaderTest, InferOutputSchemaNumeric) {
  PostgresType record = MakeRecordType(PostgresTypeId::kNumeric);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  // Numeric maps to string
  EXPECT_STREQ(schema->children[0]->format, "u");
}

TEST(CopyReaderTest, InferOutputSchemaJsonb) {
  PostgresType record = MakeRecordType(PostgresTypeId::kJsonb);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->children[0]->format, "u");
}

// ---------------------------------------------------------------------------
// MakeCopyFieldReader factory - additional types
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, MakeFieldReaderDate) {
  PostgresType pg_type(PostgresTypeId::kDate);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_DATE32);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
  EXPECT_NE(reader, nullptr);
}

TEST(CopyReaderTest, MakeFieldReaderTime) {
  PostgresType pg_type(PostgresTypeId::kTime);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeDateTime(schema.get(), NANOARROW_TYPE_TIME64,
                                       NANOARROW_TIME_UNIT_MICRO, nullptr),
            NANOARROW_OK);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
  EXPECT_NE(reader, nullptr);
}

TEST(CopyReaderTest, MakeFieldReaderTimestamp) {
  PostgresType pg_type(PostgresTypeId::kTimestamp);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeDateTime(schema.get(), NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_MICRO, nullptr),
            NANOARROW_OK);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
  EXPECT_NE(reader, nullptr);
}

TEST(CopyReaderTest, MakeFieldReaderTimestamptz) {
  PostgresType pg_type(PostgresTypeId::kTimestamptz);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeDateTime(schema.get(), NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_MICRO, "UTC"),
            NANOARROW_OK);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
  EXPECT_NE(reader, nullptr);
}

TEST(CopyReaderTest, MakeFieldReaderInterval) {
  PostgresType pg_type(PostgresTypeId::kInterval);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
  EXPECT_NE(reader, nullptr);
}

TEST(CopyReaderTest, MakeFieldReaderNumeric) {
  PostgresType pg_type(PostgresTypeId::kNumeric);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_STRING);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
  EXPECT_NE(reader, nullptr);
}

TEST(CopyReaderTest, MakeFieldReaderJsonb) {
  PostgresType pg_type(PostgresTypeId::kJsonb);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_STRING);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
  EXPECT_NE(reader, nullptr);
}

TEST(CopyReaderTest, MakeFieldReaderJson) {
  PostgresType pg_type(PostgresTypeId::kJson);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_STRING);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
  EXPECT_NE(reader, nullptr);
}

TEST(CopyReaderTest, MakeFieldReaderEnum) {
  PostgresType pg_type(PostgresTypeId::kEnum);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_STRING);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), NANOARROW_OK);
  EXPECT_NE(reader, nullptr);
}

TEST(CopyReaderTest, MakeFieldReaderArray) {
  PostgresType array_type = PostgresType(PostgresTypeId::kInt4).Array(0, "_int4");

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_LIST);
  ArrowSchemaInitFromType(schema->children[0], NANOARROW_TYPE_INT32);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(array_type, schema.get(), &reader, &error), NANOARROW_OK);
  EXPECT_NE(reader, nullptr);
}

TEST(CopyReaderTest, MakeFieldReaderRecord) {
  PostgresType record_type(PostgresTypeId::kRecord);
  record_type.AppendChild("f1", PostgresType(PostgresTypeId::kInt4));
  record_type.AppendChild("f2", PostgresType(PostgresTypeId::kFloat8));

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ArrowSchemaSetTypeStruct(schema.get(), 2);
  ArrowSchemaInitFromType(schema->children[0], NANOARROW_TYPE_INT32);
  ArrowSchemaSetName(schema->children[0], "f1");
  ArrowSchemaInitFromType(schema->children[1], NANOARROW_TYPE_DOUBLE);
  ArrowSchemaSetName(schema->children[1], "f2");

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(record_type, schema.get(), &reader, &error), NANOARROW_OK);
  EXPECT_NE(reader, nullptr);
}

TEST(CopyReaderTest, MakeFieldReaderRecordChildCountMismatch) {
  PostgresType record_type(PostgresTypeId::kRecord);
  record_type.AppendChild("f1", PostgresType(PostgresTypeId::kInt4));

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ArrowSchemaSetTypeStruct(schema.get(), 2);
  ArrowSchemaInitFromType(schema->children[0], NANOARROW_TYPE_INT32);
  ArrowSchemaSetName(schema->children[0], "f1");
  ArrowSchemaInitFromType(schema->children[1], NANOARROW_TYPE_DOUBLE);
  ArrowSchemaSetName(schema->children[1], "f2");

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(record_type, schema.get(), &reader, &error), EINVAL);
}

TEST(CopyReaderTest, MakeFieldReaderArrayChildCountInvalid) {
  // Array type with no children should error
  PostgresType bad_array(PostgresTypeId::kArray);

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_LIST);
  ArrowSchemaInitFromType(schema->children[0], NANOARROW_TYPE_INT32);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(bad_array, schema.get(), &reader, &error), EINVAL);
}

// ---------------------------------------------------------------------------
// Read Numeric with precision/scale (NUMERIC(16,10))
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadNumericWithPrecision) {
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kNumeric));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyNumeric16_10, sizeof(kTestPgCopyNumeric16_10)),
            NANOARROW_OK);

  auto* array = tester.array();
  ASSERT_EQ(array->length, 7);
  ASSERT_EQ(array->n_children, 1);

  auto* col = array->children[0];
  EXPECT_EQ(col->length, 7);
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);

  auto get_str = [&](int64_t i) -> std::string {
    return std::string(str_data + offsets[i], offsets[i + 1] - offsets[i]);
  };

  EXPECT_EQ(get_str(0), "0.0000000000");
  EXPECT_EQ(get_str(1), "1.0123400000");
  EXPECT_EQ(get_str(2), "1.0123456789");
  EXPECT_EQ(get_str(3), "-1.0123400000");
  EXPECT_EQ(get_str(4), "-1.0123456789");
  EXPECT_EQ(get_str(5), "nan");
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Read multiple record types via GetArray lifecycle
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, GetArrayWithoutRecord) {
  PostgresType record = MakeRecordType(PostgresTypeId::kBool);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("PostgreSQL", nullptr), NANOARROW_OK);
  ASSERT_EQ(reader.InitFieldReaders(nullptr), NANOARROW_OK);

  // GetArray without reading any records should return EINVAL
  nanoarrow::UniqueArray array;
  ArrowError error;
  EXPECT_EQ(reader.GetArray(array.get(), &error), EINVAL);
}

// ---------------------------------------------------------------------------
// JSONB version byte edge cases
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadJsonbWrongVersionByte) {
  // Manually construct binary COPY data with version byte != 1
  // PG COPY header + 1 field row with JSONB data using version byte 0x02
  static const uint8_t kTestPgCopyJsonbBadVersion[] = {
      0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00,  // signature
      0x00, 0x00, 0x00, 0x00,  // flags
      0x00, 0x00, 0x00, 0x00,  // extension length
      0x00, 0x01,              // field count = 1
      0x00, 0x00, 0x00, 0x04,  // field length = 4
      0x02,                    // WRONG version byte (should be 0x01)
      0x7b, 0x7d, 0x0a,       // {}  + newline (garbage after version)
      0xff, 0xff               // trailer
  };

  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kJsonb));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  // The reader sets an error message but returns NANOARROW_OK (current behavior)
  // This exercises the version byte check code path
  tester.ReadAll(kTestPgCopyJsonbBadVersion, sizeof(kTestPgCopyJsonbBadVersion));
}

// ---------------------------------------------------------------------------
// Numeric special values (NaN, +Inf, -Inf)
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadNumericSpecialValues) {
  // kTestPgCopyNumeric contains: 1000000, 0.00001234, 1.0000, -123.456,
  // 123.456, NaN, -Inf, +Inf, NULL
  CopyReaderTester tester(MakeRecordType(PostgresTypeId::kNumeric));
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyNumeric, sizeof(kTestPgCopyNumeric)), NANOARROW_OK);

  struct ArrowArray* array = tester.array();
  ASSERT_EQ(array->length, 9);
  ASSERT_EQ(array->n_children, 1);

  auto* col = array->children[0];
  auto get_str = [&](int64_t i) -> std::string {
    auto* offsets =
        reinterpret_cast<const int32_t*>(col->buffers[1]);
    auto* data = reinterpret_cast<const char*>(col->buffers[2]);
    return std::string(data + offsets[i], offsets[i + 1] - offsets[i]);
  };

  EXPECT_EQ(get_str(0), "1000000");
  EXPECT_EQ(get_str(1), "0.00001234");
  EXPECT_EQ(get_str(2), "1.0000");
  EXPECT_EQ(get_str(3), "-123.456");
  EXPECT_EQ(get_str(4), "123.456");
  // Special values
  EXPECT_EQ(get_str(5), "nan");
  EXPECT_EQ(get_str(6), "-inf");
  EXPECT_EQ(get_str(7), "inf");
  // Index 8 is NULL
  EXPECT_EQ(col->null_count, 1);
}

// ---------------------------------------------------------------------------
// Array with zero dimensions (empty array)
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadArrayZeroDim) {
  // PG COPY binary for an empty int4 array (ndim=0)
  static const uint8_t kTestPgCopyArrayEmpty[] = {
      0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00,  // signature
      0x00, 0x00, 0x00, 0x00,  // flags
      0x00, 0x00, 0x00, 0x00,  // extension length
      0x00, 0x01,              // field count = 1
      0x00, 0x00, 0x00, 0x0c,  // field length = 12
      0x00, 0x00, 0x00, 0x00,  // ndim = 0
      0x00, 0x00, 0x00, 0x00,  // has_null flags = 0
      0x00, 0x00, 0x00, 0x17,  // element OID = 23 (int4)
      0xff, 0xff               // trailer
  };

  PostgresType array_type = PostgresType(PostgresTypeId::kInt4).Array(0, "_int4");
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", array_type);

  CopyReaderTester tester(record);
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(kTestPgCopyArrayEmpty, sizeof(kTestPgCopyArrayEmpty)),
            NANOARROW_OK);

  struct ArrowArray* array = tester.array();
  ASSERT_EQ(array->length, 1);
  // The single row should be an empty list
  auto* list_col = array->children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(list_col->buffers[1]);
  EXPECT_EQ(offsets[0], 0);
  EXPECT_EQ(offsets[1], 0);  // empty list
}

// ---------------------------------------------------------------------------
// Array with negative dimensions (error path)
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, ReadArrayNegativeDim) {
  // PG COPY binary for array with ndim=-1 (should error)
  static const uint8_t kTestPgCopyArrayNegDim[] = {
      0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00,  // signature
      0x00, 0x00, 0x00, 0x00,  // flags
      0x00, 0x00, 0x00, 0x00,  // extension length
      0x00, 0x01,              // field count = 1
      0x00, 0x00, 0x00, 0x0c,  // field length = 12
      0xff, 0xff, 0xff, 0xff,  // ndim = -1
      0x00, 0x00, 0x00, 0x00,  // has_null flags
      0x00, 0x00, 0x00, 0x17,  // element OID = 23 (int4)
      0xff, 0xff               // trailer
  };

  PostgresType array_type = PostgresType(PostgresTypeId::kInt4).Array(0, "_int4");
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", array_type);

  CopyReaderTester tester(record);
  ASSERT_EQ(tester.Init(), NANOARROW_OK);
  EXPECT_EQ(tester.ReadAll(kTestPgCopyArrayNegDim, sizeof(kTestPgCopyArrayNegDim)),
            EINVAL);
}

// ---------------------------------------------------------------------------
// MakeCopyFieldReader unsupported type combination
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, MakeFieldReaderUnsupportedTypeCombination) {
  // Try to read a boolean PG type into a TIMESTAMP schema → ErrorCantConvert
  PostgresType pg_type(PostgresTypeId::kBool);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ArrowSchemaSetTypeDateTime(schema.get(), NANOARROW_TYPE_TIMESTAMP,
                             NANOARROW_TIME_UNIT_MICRO, nullptr);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), EINVAL);
}

TEST(CopyReaderTest, MakeFieldReaderTextToInt32) {
  // Try to read a text PG type into INT32 schema → ErrorCantConvert
  PostgresType pg_type(PostgresTypeId::kText);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INT32);

  std::unique_ptr<PostgresCopyFieldReader> reader;
  ArrowError error;
  EXPECT_EQ(MakeCopyFieldReader(pg_type, schema.get(), &reader, &error), EINVAL);
}

// ---------------------------------------------------------------------------
// InferOutputSchema: vendor name "Hologres" vs "PostgreSQL"
// ---------------------------------------------------------------------------

TEST(CopyReaderTest, InferOutputSchemaHologresVendor) {
  PostgresType record = MakeRecordType(PostgresTypeId::kJsonb);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("Hologres", nullptr), NANOARROW_OK);
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  // JSONB maps to STRING in both vendors
  EXPECT_STREQ(schema->children[0]->format, "u");
}

TEST(CopyReaderTest, InferOutputSchemaTimestamptzHologres) {
  PostgresType record = MakeRecordType(PostgresTypeId::kTimestamptz);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("Hologres", nullptr), NANOARROW_OK);
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  // Timestamptz → TIMESTAMP with UTC timezone
  EXPECT_STREQ(schema->children[0]->format, "tsu:UTC");
}

TEST(CopyReaderTest, InferOutputSchemaIntervalHologres) {
  PostgresType record = MakeRecordType(PostgresTypeId::kInterval);
  PostgresCopyStreamReader reader;
  ASSERT_EQ(reader.Init(record), NANOARROW_OK);
  ASSERT_EQ(reader.InferOutputSchema("Hologres", nullptr), NANOARROW_OK);
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(reader.GetSchema(schema.get()), NANOARROW_OK);
  // Interval → INTERVAL_MONTH_DAY_NANO
  EXPECT_STREQ(schema->children[0]->format, "tin");
}

}  // namespace adbcpq
