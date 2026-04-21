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
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.hpp>

#include "copy/reader.h"
#include "copy/writer.h"
#include "postgres_type.h"
#include "validation/adbc_validation_util.h"

namespace adbcpq {

// ---------------------------------------------------------------------------
// MockTypeResolver - a self-contained type resolver for offline tests
// ---------------------------------------------------------------------------

class MockTypeResolver : public PostgresTypeResolver {
 public:
  ArrowErrorCode Init() {
    auto all_types = PostgresTypeIdAll(false);
    PostgresTypeResolver::Item item;
    item.oid = 0;

    for (auto type_id : all_types) {
      std::string typreceive = PostgresTyprecv(type_id);
      std::string typname = PostgresTypname(type_id);
      item.oid++;
      item.typname = typname.c_str();
      item.typreceive = typreceive.c_str();
      NANOARROW_RETURN_NOT_OK(Insert(item, nullptr));
    }

    // Insert an array type for int4
    item.oid++;
    item.typname = "_int4";
    item.typreceive = "array_recv";
    item.child_oid = GetOID(PostgresTypeId::kInt4);
    NANOARROW_RETURN_NOT_OK(Insert(item, nullptr));

    return NANOARROW_OK;
  }
};

// ---------------------------------------------------------------------------
// CopyWriterTester - helper that wraps PostgresCopyStreamWriter
// ---------------------------------------------------------------------------

class CopyWriterTester {
 public:
  ArrowErrorCode Init(struct ArrowSchema* schema, struct ArrowArray* array,
                      const PostgresTypeResolver& resolver,
                      const std::vector<PostgresType>& pg_types) {
    NANOARROW_RETURN_NOT_OK(writer_.Init(schema));
    NANOARROW_RETURN_NOT_OK(writer_.InitFieldWriters(resolver, pg_types, &na_error_));
    NANOARROW_RETURN_NOT_OK(writer_.SetArray(array));
    NANOARROW_RETURN_NOT_OK(writer_.WriteHeader(&na_error_));
    return NANOARROW_OK;
  }

  ArrowErrorCode WriteAll(ArrowError* error) {
    int result;
    do {
      result = writer_.WriteRecord(&na_error_);
    } while (result == NANOARROW_OK);
    if (error) {
      std::memcpy(error, &na_error_, sizeof(ArrowError));
    }
    return result;
  }

  // Returns a contiguous copy of the writer buffer plus the COPY binary trailer.
  // The writer does not produce the int16(-1) trailer itself; in production
  // the caller appends it. The reader requires it to signal end-of-stream.
  std::vector<uint8_t> FinishBuffer() const {
    const auto& buf = writer_.WriteBuffer();
    std::vector<uint8_t> out(buf.size_bytes + 2);
    std::memcpy(out.data(), buf.data, buf.size_bytes);
    // Append int16(-1) in network byte order: 0xFF 0xFF
    out[buf.size_bytes] = 0xFF;
    out[buf.size_bytes + 1] = 0xFF;
    return out;
  }

  const struct ArrowBuffer& WriteBuffer() const { return writer_.WriteBuffer(); }
  const ArrowError& error() const { return na_error_; }

 private:
  PostgresCopyStreamWriter writer_;
  ArrowError na_error_;
};

// ---------------------------------------------------------------------------
// CopyReaderTester - helper that wraps PostgresCopyStreamReader
// ---------------------------------------------------------------------------

class CopyReaderTester {
 public:
  explicit CopyReaderTester(PostgresType pg_type) : pg_type_(std::move(pg_type)) {}

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

 private:
  PostgresType pg_type_;
  PostgresCopyStreamReader reader_;
  nanoarrow::UniqueArray array_;
  ArrowError na_error_;
};

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class CopyWriterTest : public ::testing::Test {
 protected:
  void SetUp() override { ASSERT_EQ(resolver_.Init(), NANOARROW_OK); }
  MockTypeResolver resolver_;
};

// ===========================================================================
// Basic type tests
// ===========================================================================

TEST_F(CopyWriterTest, WriteBoolean) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_BOOL}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<bool>(&schema.value, &array.value, &na_error,
                                              {true, false, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresType pg_type(PostgresTypeId::kBool);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kBool));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  auto* col = result->children[0];
  EXPECT_EQ(col->length, 3);
  auto* data = reinterpret_cast<const uint8_t*>(col->buffers[1]);
  EXPECT_TRUE(ArrowBitGet(data, 0));   // true
  EXPECT_FALSE(ArrowBitGet(data, 1));  // false
  // Third value is null, verified by null_count
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteInt8) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT8}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int8_t>(
                &schema.value, &array.value, &na_error,
                {int8_t{-123}, int8_t{-1}, int8_t{0}, int8_t{1}, int8_t{123},
                 std::nullopt}),
            ADBC_STATUS_OK);

  // int8 maps to int2 in postgres
  PostgresType pg_type(PostgresTypeId::kInt2);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kInt2));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 6);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const int16_t*>(col->buffers[1]);
  EXPECT_EQ(data[0], -123);
  EXPECT_EQ(data[1], -1);
  EXPECT_EQ(data[2], 0);
  EXPECT_EQ(data[3], 1);
  EXPECT_EQ(data[4], 123);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteInt16) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT16}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int16_t>(
                &schema.value, &array.value, &na_error,
                {int16_t{-123}, int16_t{-1}, int16_t{1}, int16_t{123}, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresType pg_type(PostgresTypeId::kInt2);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kInt2));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 5);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const int16_t*>(col->buffers[1]);
  EXPECT_EQ(data[0], -123);
  EXPECT_EQ(data[1], -1);
  EXPECT_EQ(data[2], 1);
  EXPECT_EQ(data[3], 123);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteInt32) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT32}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int32_t>(
                &schema.value, &array.value, &na_error,
                {int32_t{-123}, int32_t{-1}, int32_t{1}, int32_t{123}, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresType pg_type(PostgresTypeId::kInt4);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kInt4));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 5);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const int32_t*>(col->buffers[1]);
  EXPECT_EQ(data[0], -123);
  EXPECT_EQ(data[1], -1);
  EXPECT_EQ(data[2], 1);
  EXPECT_EQ(data[3], 123);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteUInt16) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_UINT16}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<uint16_t>(
                &schema.value, &array.value, &na_error,
                {uint16_t{0}, uint16_t{1}, uint16_t{65535}, std::nullopt}),
            ADBC_STATUS_OK);

  // uint16 maps to int4 in the writer
  PostgresType pg_type(PostgresTypeId::kInt4);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kInt4));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 4);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const int32_t*>(col->buffers[1]);
  EXPECT_EQ(data[0], 0);
  EXPECT_EQ(data[1], 1);
  EXPECT_EQ(data[2], 65535);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteInt64) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT64}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int64_t>(
                &schema.value, &array.value, &na_error,
                {int64_t{-123}, int64_t{-1}, int64_t{1}, int64_t{123}, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresType pg_type(PostgresTypeId::kInt8);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kInt8));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 5);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const int64_t*>(col->buffers[1]);
  EXPECT_EQ(data[0], -123);
  EXPECT_EQ(data[1], -1);
  EXPECT_EQ(data[2], 1);
  EXPECT_EQ(data[3], 123);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteUInt32) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_UINT32}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(
      adbc_validation::MakeBatch<uint32_t>(
          &schema.value, &array.value, &na_error,
          {uint32_t{0}, uint32_t{1}, uint32_t{4294967295U}, std::nullopt}),
      ADBC_STATUS_OK);

  // uint32 maps to int8 in the writer
  PostgresType pg_type(PostgresTypeId::kInt8);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kInt8));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 4);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const int64_t*>(col->buffers[1]);
  EXPECT_EQ(data[0], 0);
  EXPECT_EQ(data[1], 1);
  EXPECT_EQ(data[2], 4294967295LL);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteUInt64) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_UINT64}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<uint64_t>(&schema.value, &array.value, &na_error,
                                                  {uint64_t{0}, uint64_t{1},
                                                   std::nullopt}),
            ADBC_STATUS_OK);

  // uint64 maps to int8 in the writer
  PostgresType pg_type(PostgresTypeId::kInt8);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kInt8));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const int64_t*>(col->buffers[1]);
  EXPECT_EQ(data[0], 0);
  EXPECT_EQ(data[1], 1);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteFloat) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_FLOAT}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<float>(
                &schema.value, &array.value, &na_error,
                {-123.456f, -1.0f, 1.0f, 123.456f, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresType pg_type(PostgresTypeId::kFloat4);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kFloat4));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 5);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const float*>(col->buffers[1]);
  EXPECT_FLOAT_EQ(data[0], -123.456f);
  EXPECT_FLOAT_EQ(data[1], -1.0f);
  EXPECT_FLOAT_EQ(data[2], 1.0f);
  EXPECT_FLOAT_EQ(data[3], 123.456f);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteDouble) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_DOUBLE}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<double>(
                &schema.value, &array.value, &na_error,
                {-123.456, -1.0, 1.0, 123.456, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresType pg_type(PostgresTypeId::kFloat8);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kFloat8));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 5);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const double*>(col->buffers[1]);
  EXPECT_DOUBLE_EQ(data[0], -123.456);
  EXPECT_DOUBLE_EQ(data[1], -1.0);
  EXPECT_DOUBLE_EQ(data[2], 1.0);
  EXPECT_DOUBLE_EQ(data[3], 123.456);
  EXPECT_EQ(col->null_count, 1);
}

// ===========================================================================
// Date/Time type tests
// ===========================================================================

TEST_F(CopyWriterTest, WriteDate32) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_DATE32}}),
            ADBC_STATUS_OK);
  // 0 = 1970-01-01, 10957 = 2000-01-01, -10957 = 1940-01-02
  ASSERT_EQ(adbc_validation::MakeBatch<int32_t>(
                &schema.value, &array.value, &na_error,
                {int32_t{0}, int32_t{10957}, int32_t{-10957}, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresType pg_type(PostgresTypeId::kDate);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  // The writer subtracts 10957 (epoch offset); the reader adds it back.
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kDate));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 4);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const int32_t*>(col->buffers[1]);
  EXPECT_EQ(data[0], 0);
  EXPECT_EQ(data[1], 10957);
  EXPECT_EQ(data[2], -10957);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteTime64Micro) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(
      ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_TIME64,
                                 NANOARROW_TIME_UNIT_MICRO, /*timezone=*/nullptr),
      NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 0), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 86399999999LL), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendNull(array.value.children[0], 1), NANOARROW_OK);
  array.value.length = 3;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kTime);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kTime));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const int64_t*>(col->buffers[1]);
  EXPECT_EQ(data[0], 0);
  EXPECT_EQ(data[1], 86399999999LL);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteTimestampMicro) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(
      ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_TIMESTAMP,
                                 NANOARROW_TIME_UNIT_MICRO, /*timezone=*/nullptr),
      NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 0), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 946684800000000LL),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendNull(array.value.children[0], 1), NANOARROW_OK);
  array.value.length = 3;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kTimestamp);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  // The reader reads timestamp as micro with epoch offset
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kTimestamp));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const int64_t*>(col->buffers[1]);
  EXPECT_EQ(data[0], 0);
  EXPECT_EQ(data[1], 946684800000000LL);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteTimestampMilli) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(
      ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_TIMESTAMP,
                                 NANOARROW_TIME_UNIT_MILLI, /*timezone=*/nullptr),
      NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 0), NANOARROW_OK);
  // 946684800000 ms = 2000-01-01T00:00:00 in milliseconds
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 946684800000LL), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendNull(array.value.children[0], 1), NANOARROW_OK);
  array.value.length = 3;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kTimestamp);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kTimestamp));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const int64_t*>(col->buffers[1]);
  // The writer converts millis to micros: 0ms -> 0us, 946684800000ms -> 946684800000000us
  EXPECT_EQ(data[0], 0);
  EXPECT_EQ(data[1], 946684800000000LL);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteTimestampSecond) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(
      ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_TIMESTAMP,
                                 NANOARROW_TIME_UNIT_SECOND, /*timezone=*/nullptr),
      NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 0), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 946684800LL), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendNull(array.value.children[0], 1), NANOARROW_OK);
  array.value.length = 3;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kTimestamp);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kTimestamp));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const int64_t*>(col->buffers[1]);
  // seconds -> micros: 946684800s -> 946684800000000us
  EXPECT_EQ(data[0], 0);
  EXPECT_EQ(data[1], 946684800000000LL);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteTimestampNano) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(
      ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_TIMESTAMP,
                                 NANOARROW_TIME_UNIT_NANO, /*timezone=*/nullptr),
      NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 0), NANOARROW_OK);
  // 946684800000000000 ns = 2000-01-01T00:00:00 in nanoseconds
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 946684800000000000LL),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendNull(array.value.children[0], 1), NANOARROW_OK);
  array.value.length = 3;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kTimestamp);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kTimestamp));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  auto* col = result->children[0];
  auto* data = reinterpret_cast<const int64_t*>(col->buffers[1]);
  // nanos -> micros: 946684800000000000ns / 1000 = 946684800000000us
  EXPECT_EQ(data[0], 0);
  EXPECT_EQ(data[1], 946684800000000LL);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteTimestampOverflow) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(
      ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_TIMESTAMP,
                                 NANOARROW_TIME_UNIT_SECOND, /*timezone=*/nullptr),
      NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  // Value larger than kMaxSafeSecondsToMicros
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0],
                                (std::numeric_limits<int64_t>::max)()),
            NANOARROW_OK);
  array.value.length = 1;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kTimestamp);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ArrowError write_error;
  EXPECT_NE(writer.WriteAll(&write_error), ENODATA);
}

TEST_F(CopyWriterTest, WriteTimestampUnderflow) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(
      ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_TIMESTAMP,
                                 NANOARROW_TIME_UNIT_MICRO, /*timezone=*/nullptr),
      NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  // Value that would underflow when subtracting the postgres epoch
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0],
                                (std::numeric_limits<int64_t>::min)()),
            NANOARROW_OK);
  array.value.length = 1;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kTimestamp);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ArrowError write_error;
  EXPECT_NE(writer.WriteAll(&write_error), ENODATA);
}

// ===========================================================================
// Duration tests
// ===========================================================================

TEST_F(CopyWriterTest, WriteDurationMicro) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_DURATION,
                                        NANOARROW_TIME_UNIT_MICRO, /*timezone=*/nullptr),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 0), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 1000000LL), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendNull(array.value.children[0], 1), NANOARROW_OK);
  array.value.length = 3;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kInterval);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  // Duration writes as interval with days=0, months=0
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kInterval));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  // The reader reads interval as INTERVAL_MONTH_DAY_NANO
  // The writer writes micros directly, the reader converts usec->ns
  auto* col = result->children[0];
  auto* raw_data = reinterpret_cast<const uint8_t*>(col->buffers[1]);
  // Interval is stored as {months(4), days(4), ns(8)} = 16 bytes per value
  // Value 0: months=0, days=0, ns=0
  int32_t months_0, days_0;
  int64_t ns_0;
  std::memcpy(&months_0, raw_data + 0 * 16, 4);
  std::memcpy(&days_0, raw_data + 0 * 16 + 4, 4);
  std::memcpy(&ns_0, raw_data + 0 * 16 + 8, 8);
  EXPECT_EQ(months_0, 0);
  EXPECT_EQ(days_0, 0);
  EXPECT_EQ(ns_0, 0);
  // Value 1: months=0, days=0, ns=1000000*1000=1000000000
  int32_t months_1, days_1;
  int64_t ns_1;
  std::memcpy(&months_1, raw_data + 1 * 16, 4);
  std::memcpy(&days_1, raw_data + 1 * 16 + 4, 4);
  std::memcpy(&ns_1, raw_data + 1 * 16 + 8, 8);
  EXPECT_EQ(months_1, 0);
  EXPECT_EQ(days_1, 0);
  EXPECT_EQ(ns_1, 1000000000LL);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteDurationMilli) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_DURATION,
                                        NANOARROW_TIME_UNIT_MILLI, /*timezone=*/nullptr),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 0), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 1000LL), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendNull(array.value.children[0], 1), NANOARROW_OK);
  array.value.length = 3;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kInterval);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kInterval));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  auto* col = result->children[0];
  auto* raw_data = reinterpret_cast<const uint8_t*>(col->buffers[1]);
  // 1000ms -> 1000*1000=1000000us -> reader converts to ns: 1000000*1000=1000000000ns
  int64_t ns_1;
  std::memcpy(&ns_1, raw_data + 1 * 16 + 8, 8);
  EXPECT_EQ(ns_1, 1000000000LL);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteDurationSecond) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_DURATION,
                                        NANOARROW_TIME_UNIT_SECOND, /*timezone=*/nullptr),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 0), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 1LL), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendNull(array.value.children[0], 1), NANOARROW_OK);
  array.value.length = 3;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kInterval);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kInterval));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  auto* col = result->children[0];
  auto* raw_data = reinterpret_cast<const uint8_t*>(col->buffers[1]);
  // 1s -> 1*1000000=1000000us -> reader: 1000000*1000=1000000000ns
  int64_t ns_1;
  std::memcpy(&ns_1, raw_data + 1 * 16 + 8, 8);
  EXPECT_EQ(ns_1, 1000000000LL);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteDurationNano) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_DURATION,
                                        NANOARROW_TIME_UNIT_NANO, /*timezone=*/nullptr),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 0), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0], 1000000000LL), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendNull(array.value.children[0], 1), NANOARROW_OK);
  array.value.length = 3;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kInterval);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kInterval));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  auto* col = result->children[0];
  auto* raw_data = reinterpret_cast<const uint8_t*>(col->buffers[1]);
  // 1000000000ns / 1000 = 1000000us -> reader: 1000000*1000=1000000000ns
  int64_t ns_1;
  std::memcpy(&ns_1, raw_data + 1 * 16 + 8, 8);
  EXPECT_EQ(ns_1, 1000000000LL);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteDurationOverflow) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_DURATION,
                                        NANOARROW_TIME_UNIT_SECOND, /*timezone=*/nullptr),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0],
                                (std::numeric_limits<int64_t>::max)()),
            NANOARROW_OK);
  array.value.length = 1;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kInterval);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ArrowError write_error;
  EXPECT_NE(writer.WriteAll(&write_error), ENODATA);
}

// ===========================================================================
// Interval test
// ===========================================================================

TEST_F(CopyWriterTest, WriteInterval) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetType(schema->children[0],
                                NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);

  struct ArrowInterval interval;
  ArrowIntervalInit(&interval, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
  interval.months = 14;
  interval.days = 32;
  interval.ns = 1234000;  // 1234 microseconds in nanoseconds
  ASSERT_EQ(ArrowArrayAppendInterval(array.value.children[0], &interval), NANOARROW_OK);
  array.value.length = 1;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kInterval);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kInterval));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 1);
  auto* col = result->children[0];
  auto* raw_data = reinterpret_cast<const uint8_t*>(col->buffers[1]);
  int32_t months_out, days_out;
  int64_t ns_out;
  std::memcpy(&months_out, raw_data, 4);
  std::memcpy(&days_out, raw_data + 4, 4);
  std::memcpy(&ns_out, raw_data + 8, 8);
  EXPECT_EQ(months_out, 14);
  EXPECT_EQ(days_out, 32);
  // Writer divides ns by 1000 to get us (1234000/1000=1234),
  // Reader multiplies us by 1000 to get ns (1234*1000=1234000)
  EXPECT_EQ(ns_out, 1234000LL);
}

// ===========================================================================
// Decimal tests
// ===========================================================================

TEST_F(CopyWriterTest, WriteDecimal128Simple) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetTypeDecimal(schema->children[0], NANOARROW_TYPE_DECIMAL128, 10,
                                       2),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);

  struct ArrowDecimal decimal;
  ArrowDecimalInit(&decimal, 128, 10, 2);
  ArrowDecimalSetInt(&decimal, 12345);  // Represents 123.45
  ASSERT_EQ(ArrowArrayAppendDecimal(array.value.children[0], &decimal), NANOARROW_OK);
  array.value.length = 1;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kNumeric);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  // Read back as NUMERIC -> string
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kNumeric));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 1);
  auto* col = result->children[0];
  // Numeric is read as string
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);
  std::string value(str_data + offsets[0], offsets[1] - offsets[0]);
  EXPECT_EQ(value, "123.45");
}

TEST_F(CopyWriterTest, WriteDecimal128Negative) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetTypeDecimal(schema->children[0], NANOARROW_TYPE_DECIMAL128, 10,
                                       2),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);

  struct ArrowDecimal decimal;
  ArrowDecimalInit(&decimal, 128, 10, 2);
  ArrowDecimalSetInt(&decimal, -12345);  // Represents -123.45
  ASSERT_EQ(ArrowArrayAppendDecimal(array.value.children[0], &decimal), NANOARROW_OK);
  array.value.length = 1;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kNumeric);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kNumeric));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 1);
  auto* col = result->children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);
  std::string value(str_data + offsets[0], offsets[1] - offsets[0]);
  EXPECT_EQ(value, "-123.45");
}

TEST_F(CopyWriterTest, WriteDecimal128Zero) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetTypeDecimal(schema->children[0], NANOARROW_TYPE_DECIMAL128, 10,
                                       2),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);

  struct ArrowDecimal decimal;
  ArrowDecimalInit(&decimal, 128, 10, 2);
  ArrowDecimalSetInt(&decimal, 0);
  ASSERT_EQ(ArrowArrayAppendDecimal(array.value.children[0], &decimal), NANOARROW_OK);
  array.value.length = 1;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kNumeric);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kNumeric));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 1);
  auto* col = result->children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);
  std::string value(str_data + offsets[0], offsets[1] - offsets[0]);
  EXPECT_EQ(value, "0");
}

TEST_F(CopyWriterTest, WriteDecimal128SmallFractional) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetTypeDecimal(schema->children[0], NANOARROW_TYPE_DECIMAL128, 10,
                                       5),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);

  struct ArrowDecimal decimal;
  ArrowDecimalInit(&decimal, 128, 10, 5);
  ArrowDecimalSetInt(&decimal, 123);  // Represents 0.00123
  ASSERT_EQ(ArrowArrayAppendDecimal(array.value.children[0], &decimal), NANOARROW_OK);
  array.value.length = 1;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kNumeric);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kNumeric));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 1);
  auto* col = result->children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);
  std::string value(str_data + offsets[0], offsets[1] - offsets[0]);
  EXPECT_EQ(value, "0.00123");
}

TEST_F(CopyWriterTest, WriteDecimal128NegativeScale) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetTypeDecimal(schema->children[0], NANOARROW_TYPE_DECIMAL128, 10,
                                       -2),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);

  struct ArrowDecimal decimal;
  ArrowDecimalInit(&decimal, 128, 10, -2);
  ArrowDecimalSetInt(&decimal, 123);  // Represents 12300
  ASSERT_EQ(ArrowArrayAppendDecimal(array.value.children[0], &decimal), NANOARROW_OK);
  array.value.length = 1;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kNumeric);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kNumeric));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 1);
  auto* col = result->children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);
  std::string value(str_data + offsets[0], offsets[1] - offsets[0]);
  EXPECT_EQ(value, "12300");
}

// ===========================================================================
// String/Binary tests
// ===========================================================================

TEST_F(CopyWriterTest, WriteString) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_STRING}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<std::string>(
                &schema.value, &array.value, &na_error,
                {std::string("hello"), std::string(""), std::nullopt}),
            ADBC_STATUS_OK);

  PostgresType pg_type(PostgresTypeId::kText);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kText));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  auto* col = result->children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);
  std::string val0(str_data + offsets[0], offsets[1] - offsets[0]);
  std::string val1(str_data + offsets[1], offsets[2] - offsets[1]);
  EXPECT_EQ(val0, "hello");
  EXPECT_EQ(val1, "");
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteLargeString) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_LARGE_STRING),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  struct ArrowBufferView view;
  const char* hello = "hello";
  view.data.as_char = hello;
  view.size_bytes = 5;
  ASSERT_EQ(ArrowArrayAppendBytes(array.value.children[0], view), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendNull(array.value.children[0], 1), NANOARROW_OK);
  array.value.length = 2;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kText);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kText));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 2);
  auto* col = result->children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);
  std::string val0(str_data + offsets[0], offsets[1] - offsets[0]);
  EXPECT_EQ(val0, "hello");
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteBinary) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_BINARY}}),
            ADBC_STATUS_OK);
  std::vector<std::byte> data_bytes = {std::byte{0x01}, std::byte{0x02},
                                       std::byte{0x03}};
  ASSERT_EQ(adbc_validation::MakeBatch<std::vector<std::byte>>(
                &schema.value, &array.value, &na_error,
                {data_bytes, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresType pg_type(PostgresTypeId::kBytea);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kBytea));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 2);
  auto* col = result->children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* bin_data = reinterpret_cast<const uint8_t*>(col->buffers[2]);
  EXPECT_EQ(offsets[1] - offsets[0], 3);
  EXPECT_EQ(bin_data[0], 0x01);
  EXPECT_EQ(bin_data[1], 0x02);
  EXPECT_EQ(bin_data[2], 0x03);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteLargeBinary) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_LARGE_BINARY),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  uint8_t raw[] = {0xDE, 0xAD, 0xBE, 0xEF};
  struct ArrowBufferView view;
  view.data.as_uint8 = raw;
  view.size_bytes = 4;
  ASSERT_EQ(ArrowArrayAppendBytes(array.value.children[0], view), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendNull(array.value.children[0], 1), NANOARROW_OK);
  array.value.length = 2;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kBytea);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kBytea));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 2);
  auto* col = result->children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* bin_data = reinterpret_cast<const uint8_t*>(col->buffers[2]);
  EXPECT_EQ(offsets[1] - offsets[0], 4);
  EXPECT_EQ(bin_data[0], 0xDE);
  EXPECT_EQ(bin_data[1], 0xAD);
  EXPECT_EQ(bin_data[2], 0xBE);
  EXPECT_EQ(bin_data[3], 0xEF);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteFixedSizeBinary) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(
      ArrowSchemaSetTypeFixedSize(schema->children[0], NANOARROW_TYPE_FIXED_SIZE_BINARY,
                                  4),
      NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  uint8_t raw[] = {0x01, 0x02, 0x03, 0x04};
  struct ArrowBufferView view;
  view.data.as_uint8 = raw;
  view.size_bytes = 4;
  ASSERT_EQ(ArrowArrayAppendBytes(array.value.children[0], view), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendNull(array.value.children[0], 1), NANOARROW_OK);
  array.value.length = 2;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kBytea);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kBytea));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 2);
  auto* col = result->children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* bin_data = reinterpret_cast<const uint8_t*>(col->buffers[2]);
  EXPECT_EQ(offsets[1] - offsets[0], 4);
  EXPECT_EQ(bin_data[0], 0x01);
  EXPECT_EQ(bin_data[1], 0x02);
  EXPECT_EQ(bin_data[2], 0x03);
  EXPECT_EQ(bin_data[3], 0x04);
  EXPECT_EQ(col->null_count, 1);
}

// ===========================================================================
// JSONB tests
// ===========================================================================

TEST_F(CopyWriterTest, WriteJsonb) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_STRING}}),
            ADBC_STATUS_OK);
  std::string json_val = R"({"key":"value"})";
  ASSERT_EQ(adbc_validation::MakeBatch<std::string>(
                &schema.value, &array.value, &na_error, {json_val}),
            ADBC_STATUS_OK);

  // Use kJsonb to trigger the JSONB writer
  PostgresType pg_type(PostgresTypeId::kJsonb);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  // Verify round-trip through the JSONB reader
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kJsonb));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 1);
  auto* col = result->children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);
  std::string value(str_data + offsets[0], offsets[1] - offsets[0]);
  EXPECT_EQ(value, json_val);
}

TEST_F(CopyWriterTest, WriteJsonbNull) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_STRING}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<std::string>(
                &schema.value, &array.value, &na_error,
                {std::optional<std::string>(std::nullopt)}),
            ADBC_STATUS_OK);

  PostgresType pg_type(PostgresTypeId::kJsonb);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kJsonb));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 1);
  auto* col = result->children[0];
  EXPECT_EQ(col->null_count, 1);
}

// ===========================================================================
// Dictionary tests
// ===========================================================================

TEST_F(CopyWriterTest, WriteDictionaryString) {
  nanoarrow::UniqueSchema schema;
  nanoarrow::UniqueArray array;
  struct ArrowError na_error;

  // Create a struct with one dictionary-encoded string child
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT32), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateDictionary(schema->children[0]), NANOARROW_OK);
  ASSERT_EQ(
      ArrowSchemaInitFromType(schema->children[0]->dictionary, NANOARROW_TYPE_STRING),
      NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  // Build the struct array manually: 1 child which is a dict array
  ASSERT_EQ(ArrowArrayInitFromSchema(array.get(), schema.get(), &na_error),
            NANOARROW_OK)
      << na_error.message;
  ASSERT_EQ(ArrowArrayStartAppending(array.get()), NANOARROW_OK);

  // Build dictionary values
  struct ArrowBufferView bv;
  const char* val_a = "alpha";
  bv.data.as_char = val_a;
  bv.size_bytes = 5;
  ASSERT_EQ(ArrowArrayAppendBytes(array->children[0]->dictionary, bv), NANOARROW_OK);
  const char* val_b = "beta";
  bv.data.as_char = val_b;
  bv.size_bytes = 4;
  ASSERT_EQ(ArrowArrayAppendBytes(array->children[0]->dictionary, bv), NANOARROW_OK);

  // Indices: 0, 1, 0
  ASSERT_EQ(ArrowArrayAppendInt(array->children[0], 0), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array->children[0], 1), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array->children[0], 0), NANOARROW_OK);
  array->length = 3;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(array.get(), &na_error), NANOARROW_OK)
      << na_error.message;

  PostgresType pg_type(PostgresTypeId::kText);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), array.get(), resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  // Read back as text
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kText));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  auto* col = result->children[0];
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);
  std::string val0(str_data + offsets[0], offsets[1] - offsets[0]);
  std::string val1(str_data + offsets[1], offsets[2] - offsets[1]);
  std::string val2(str_data + offsets[2], offsets[3] - offsets[2]);
  EXPECT_EQ(val0, "alpha");
  EXPECT_EQ(val1, "beta");
  EXPECT_EQ(val2, "alpha");
}

TEST_F(CopyWriterTest, WriteDictionaryWithNullInDict) {
  nanoarrow::UniqueSchema schema;
  nanoarrow::UniqueArray array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT32), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateDictionary(schema->children[0]), NANOARROW_OK);
  ASSERT_EQ(
      ArrowSchemaInitFromType(schema->children[0]->dictionary, NANOARROW_TYPE_STRING),
      NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(array.get(), schema.get(), &na_error),
            NANOARROW_OK)
      << na_error.message;
  ASSERT_EQ(ArrowArrayStartAppending(array.get()), NANOARROW_OK);

  // Build dictionary with a null entry at index 1
  struct ArrowBufferView bv;
  const char* val_a = "hello";
  bv.data.as_char = val_a;
  bv.size_bytes = 5;
  ASSERT_EQ(ArrowArrayAppendBytes(array->children[0]->dictionary, bv), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendNull(array->children[0]->dictionary, 1), NANOARROW_OK);

  // Index 0 points to "hello", index 1 points to null in dict
  ASSERT_EQ(ArrowArrayAppendInt(array->children[0], 0), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array->children[0], 1), NANOARROW_OK);
  array->length = 2;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(array.get(), &na_error), NANOARROW_OK)
      << na_error.message;

  PostgresType pg_type(PostgresTypeId::kText);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), array.get(), resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kText));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 2);
  auto* col = result->children[0];
  // First value is "hello", second is null (because dict value at index 1 is null)
  auto* offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  auto* str_data = reinterpret_cast<const char*>(col->buffers[2]);
  std::string val0(str_data + offsets[0], offsets[1] - offsets[0]);
  EXPECT_EQ(val0, "hello");
  EXPECT_EQ(col->null_count, 1);
}

// ===========================================================================
// List type tests
// ===========================================================================

TEST_F(CopyWriterTest, WriteListInt32) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(
                &schema.value,
                {adbc_validation::SchemaField::Nested(
                    "col", NANOARROW_TYPE_LIST,
                    {adbc_validation::SchemaField("item", NANOARROW_TYPE_INT32)})}),
            ADBC_STATUS_OK);

  using IntVec = std::vector<int32_t>;
  ASSERT_EQ(
      adbc_validation::MakeBatch<IntVec>(
          &schema.value, &array.value, &na_error,
          {IntVec{1, 2, 3}, IntVec{4, 5}, std::nullopt}),
      ADBC_STATUS_OK);

  PostgresType child_type(PostgresTypeId::kInt4);
  PostgresType pg_type = child_type.Array(
      resolver_.GetOID(PostgresTypeId::kInt4), "_int4");
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  // Read back as array of int4
  PostgresType int4_type(PostgresTypeId::kInt4);
  int4_type = int4_type.WithPgTypeInfo(resolver_.GetOID(PostgresTypeId::kInt4), "int4");
  PostgresType array_type = int4_type.Array(0, "_int4");
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", array_type);
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  auto* col = result->children[0];
  // First list: [1, 2, 3]
  auto* list_offsets = reinterpret_cast<const int32_t*>(col->buffers[1]);
  EXPECT_EQ(list_offsets[1] - list_offsets[0], 3);
  EXPECT_EQ(list_offsets[2] - list_offsets[1], 2);
  auto* items = col->children[0];
  auto* item_data = reinterpret_cast<const int32_t*>(items->buffers[1]);
  EXPECT_EQ(item_data[0], 1);
  EXPECT_EQ(item_data[1], 2);
  EXPECT_EQ(item_data[2], 3);
  EXPECT_EQ(item_data[3], 4);
  EXPECT_EQ(item_data[4], 5);
  EXPECT_EQ(col->null_count, 1);
}

TEST_F(CopyWriterTest, WriteFixedSizeListInt32) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetTypeFixedSize(schema->children[0],
                                         NANOARROW_TYPE_FIXED_SIZE_LIST, 3),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetType(schema->children[0]->children[0], NANOARROW_TYPE_INT32),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0]->children[0], "item"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);

  // First element: [10, 20, 30]
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0]->children[0], 10), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0]->children[0], 20), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0]->children[0], 30), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayFinishElement(array.value.children[0]), NANOARROW_OK);
  // Second element: [40, 50, 60]
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0]->children[0], 40), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0]->children[0], 50), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(array.value.children[0]->children[0], 60), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayFinishElement(array.value.children[0]), NANOARROW_OK);
  array.value.length = 2;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType child_type(PostgresTypeId::kInt4);
  PostgresType pg_type = child_type.Array(
      resolver_.GetOID(PostgresTypeId::kInt4), "_int4");
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType int4_type(PostgresTypeId::kInt4);
  int4_type = int4_type.WithPgTypeInfo(resolver_.GetOID(PostgresTypeId::kInt4), "int4");
  PostgresType array_type = int4_type.Array(0, "_int4");
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", array_type);
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 2);
  auto* col = result->children[0];
  auto* items = col->children[0];
  auto* item_data = reinterpret_cast<const int32_t*>(items->buffers[1]);
  EXPECT_EQ(item_data[0], 10);
  EXPECT_EQ(item_data[1], 20);
  EXPECT_EQ(item_data[2], 30);
  EXPECT_EQ(item_data[3], 40);
  EXPECT_EQ(item_data[4], 50);
  EXPECT_EQ(item_data[5], 60);
}

// ===========================================================================
// Null type test
// ===========================================================================

TEST_F(CopyWriterTest, WriteNullType) {
  nanoarrow::UniqueSchema schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_NA), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);

  ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, schema.get(), &na_error),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&array.value), NANOARROW_OK);
  // NA-type values are always null from the array view's perspective.
  // The tuple writer sees them as null and writes -1 (null field marker),
  // so the NullFieldWriter::Write is never invoked. The write succeeds.
  array.value.children[0]->length = 2;
  array.value.length = 2;
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, &na_error), NANOARROW_OK);

  PostgresType pg_type(PostgresTypeId::kText);
  std::vector<PostgresType> pg_types = {pg_type};

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(schema.get(), &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  // Read back - all values should be null
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col", PostgresType(PostgresTypeId::kText));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 2);
  auto* col = result->children[0];
  EXPECT_EQ(col->null_count, 2);
}

// ===========================================================================
// Factory test
// ===========================================================================

TEST_F(CopyWriterTest, MakeCopyFieldWriterUnsupportedType) {
  // NANOARROW_TYPE_STRUCT is not handled by MakeCopyFieldWriter's switch
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeStruct(schema.get(), 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT32), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "child"), NANOARROW_OK);

  nanoarrow::UniqueArrayView array_view;
  ASSERT_EQ(ArrowArrayViewInitFromSchema(array_view.get(), schema.get(), nullptr),
            NANOARROW_OK);

  std::unique_ptr<PostgresCopyFieldWriter> field_writer;
  ArrowError error;
  PostgresType pg_type(PostgresTypeId::kText);
  EXPECT_EQ(MakeCopyFieldWriter(schema.get(), array_view.get(), resolver_, pg_type,
                                &field_writer, &error),
            EINVAL);
}

// ===========================================================================
// StreamWriter lifecycle tests
// ===========================================================================

TEST_F(CopyWriterTest, StreamWriterInitAndRewind) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT32}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int32_t>(
                &schema.value, &array.value, &na_error, {int32_t{42}}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriter writer;
  ASSERT_EQ(writer.Init(&schema.value), NANOARROW_OK);

  std::vector<PostgresType> pg_types = {PostgresType(PostgresTypeId::kInt4)};
  ASSERT_EQ(writer.InitFieldWriters(resolver_, pg_types, &na_error), NANOARROW_OK);

  ASSERT_EQ(writer.SetArray(&array.value), NANOARROW_OK);
  ASSERT_EQ(writer.WriteHeader(&na_error), NANOARROW_OK);

  // Write one record
  ASSERT_EQ(writer.WriteRecord(&na_error), NANOARROW_OK);
  // Next write should return ENODATA (end of records)
  EXPECT_EQ(writer.WriteRecord(&na_error), ENODATA);

  // Verify buffer has content
  const auto& buf = writer.WriteBuffer();
  EXPECT_GT(buf.size_bytes, 0);

  // Rewind and verify buffer is empty
  writer.Rewind();
  const auto& buf2 = writer.WriteBuffer();
  EXPECT_EQ(buf2.size_bytes, 0);
}

TEST_F(CopyWriterTest, StreamWriterMultiColumn) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value,
                                         {{"int_col", NANOARROW_TYPE_INT32},
                                          {"str_col", NANOARROW_TYPE_STRING},
                                          {"bool_col", NANOARROW_TYPE_BOOL}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(
      (adbc_validation::MakeBatch<int32_t, std::string, bool>(
          &schema.value, &array.value, &na_error,
          {int32_t{42}, int32_t{-1}, std::nullopt},
          {std::string("hello"), std::nullopt, std::string("world")},
          {true, false, std::nullopt})),
      ADBC_STATUS_OK);

  std::vector<PostgresType> pg_types = {
      PostgresType(PostgresTypeId::kInt4),
      PostgresType(PostgresTypeId::kText),
      PostgresType(PostgresTypeId::kBool),
  };

  CopyWriterTester writer;
  ASSERT_EQ(writer.Init(&schema.value, &array.value, resolver_, pg_types), NANOARROW_OK);
  ASSERT_EQ(writer.WriteAll(nullptr), ENODATA);

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("int_col", PostgresType(PostgresTypeId::kInt4));
  record.AppendChild("str_col", PostgresType(PostgresTypeId::kText));
  record.AppendChild("bool_col", PostgresType(PostgresTypeId::kBool));
  CopyReaderTester reader(record);
  ASSERT_EQ(reader.Init(), NANOARROW_OK);
  auto buf = writer.FinishBuffer();
  ASSERT_EQ(reader.ReadAll(buf.data(), buf.size()), NANOARROW_OK);

  auto* result = reader.array();
  ASSERT_EQ(result->length, 3);
  ASSERT_EQ(result->n_children, 3);

  // Verify int column
  auto* int_col = result->children[0];
  auto* int_data = reinterpret_cast<const int32_t*>(int_col->buffers[1]);
  EXPECT_EQ(int_data[0], 42);
  EXPECT_EQ(int_data[1], -1);
  EXPECT_EQ(int_col->null_count, 1);

  // Verify string column
  auto* str_col = result->children[1];
  auto* str_offsets = reinterpret_cast<const int32_t*>(str_col->buffers[1]);
  auto* str_raw = reinterpret_cast<const char*>(str_col->buffers[2]);
  std::string s0(str_raw + str_offsets[0], str_offsets[1] - str_offsets[0]);
  EXPECT_EQ(s0, "hello");
  EXPECT_EQ(str_col->null_count, 1);
  // Third string value: "world"
  std::string s2(str_raw + str_offsets[2], str_offsets[3] - str_offsets[2]);
  EXPECT_EQ(s2, "world");

  // Verify bool column
  auto* bool_col = result->children[2];
  auto* bool_data = reinterpret_cast<const uint8_t*>(bool_col->buffers[1]);
  EXPECT_TRUE(ArrowBitGet(bool_data, 0));
  EXPECT_FALSE(ArrowBitGet(bool_data, 1));
  EXPECT_EQ(bool_col->null_count, 1);
}

}  // namespace adbcpq
