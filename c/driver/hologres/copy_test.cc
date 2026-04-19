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

// Pre-generated PG COPY binary data from the PostgreSQL driver tests
#include "postgresql/copy/postgres_copy_test_common.h"

namespace adbcpq {

// Helper: given a PG type, raw copy data, and an expected Arrow type,
// runs the read pipeline (header + records) and returns the result array.
class CopyReaderTester {
 public:
  CopyReaderTester(PostgresType pg_type) : pg_type_(std::move(pg_type)) {}

  ArrowErrorCode Init() {
    NANOARROW_RETURN_NOT_OK(reader_.Init(pg_type_));
    NANOARROW_RETURN_NOT_OK(reader_.InferOutputSchema("PostgreSQL", &na_error_));
    NANOARROW_RETURN_NOT_OK(reader_.InitFieldReaders(&na_error_));
    return NANOARROW_OK;
  }

  ArrowErrorCode ReadAll(const uint8_t* data, size_t len) {
    ArrowBufferView view;
    view.data.as_uint8 = data;
    view.size_bytes = static_cast<int64_t>(len);

    NANOARROW_RETURN_NOT_OK(reader_.ReadHeader(&view, &na_error_));

    int result;
    do {
      result = reader_.ReadRecord(&view, &na_error_);
    } while (result == NANOARROW_OK);

    if (result != ENODATA) return result;

    NANOARROW_RETURN_NOT_OK(reader_.GetArray(array_.get(), &na_error_));
    return NANOARROW_OK;
  }

  ArrowArray* array() { return array_.get(); }
  const ArrowError& error() const { return na_error_; }
  PostgresCopyStreamReader& reader() { return reader_; }

 private:
  PostgresType pg_type_;
  PostgresCopyStreamReader reader_;
  nanoarrow::UniqueArray array_;
  ArrowError na_error_;
};

static PostgresType MakeRecordType(PostgresTypeId child_type_id) {
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(child_type_id));
  return record;
}

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

}  // namespace adbcpq
