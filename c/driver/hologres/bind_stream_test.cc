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

#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow.hpp>

#include "bind_stream.h"

namespace adbcpq {

// ---------------------------------------------------------------------------
// Helper: build a STRUCT ArrowArrayStream with int32 column and given values
// ---------------------------------------------------------------------------
static struct ArrowArrayStream MakeInt32Stream(const std::vector<int32_t>& values) {
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ArrowSchemaSetTypeStruct(schema.get(), 1);
  ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT32);
  ArrowSchemaSetName(schema->children[0], "col0");

  nanoarrow::UniqueArray array;
  ArrowArrayInitFromSchema(array.get(), schema.get(), nullptr);
  ArrowArrayStartAppending(array.get());
  for (int32_t v : values) {
    ArrowArrayAppendInt(array->children[0], v);
    ArrowArrayFinishElement(array.get());
  }
  ArrowArrayFinishBuildingDefault(array.get(), nullptr);

  struct ArrowArrayStream stream;
  ArrowBasicArrayStreamInit(&stream, schema.get(), 1);
  ArrowBasicArrayStreamSetArray(&stream, 0, array.get());
  return stream;
}

// Build an empty stream (no arrays, just schema)
static struct ArrowArrayStream MakeEmptyInt32Stream() {
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ArrowSchemaSetTypeStruct(schema.get(), 1);
  ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT32);
  ArrowSchemaSetName(schema->children[0], "col0");

  struct ArrowArrayStream stream;
  ArrowBasicArrayStreamInit(&stream, schema.get(), 0);
  return stream;
}

// Build a non-STRUCT stream (just an int32, not wrapped in struct)
static struct ArrowArrayStream MakeNonStructStream() {
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ArrowSchemaSetType(schema.get(), NANOARROW_TYPE_INT32);

  nanoarrow::UniqueArray array;
  ArrowArrayInitFromSchema(array.get(), schema.get(), nullptr);
  ArrowArrayStartAppending(array.get());
  ArrowArrayAppendInt(array.get(), 1);
  ArrowArrayFinishBuildingDefault(array.get(), nullptr);

  struct ArrowArrayStream stream;
  ArrowBasicArrayStreamInit(&stream, schema.get(), 1);
  ArrowBasicArrayStreamSetArray(&stream, 0, array.get());
  return stream;
}

// Build a multi-batch stream
static struct ArrowArrayStream MakeMultiBatchStream() {
  // Keep a separate copy of the schema for array construction,
  // because ArrowBasicArrayStreamInit moves ownership of the schema.
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ArrowSchemaSetTypeStruct(schema.get(), 1);
  ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT32);
  ArrowSchemaSetName(schema->children[0], "col0");

  nanoarrow::UniqueSchema schema_copy;
  ArrowSchemaDeepCopy(schema.get(), schema_copy.get());

  struct ArrowArrayStream stream;
  ArrowBasicArrayStreamInit(&stream, schema.get(), 3);

  for (int batch = 0; batch < 3; ++batch) {
    nanoarrow::UniqueArray array;
    ArrowArrayInitFromSchema(array.get(), schema_copy.get(), nullptr);
    ArrowArrayStartAppending(array.get());
    for (int i = 0; i < 2; ++i) {
      ArrowArrayAppendInt(array->children[0], batch * 10 + i);
      ArrowArrayFinishElement(array.get());
    }
    ArrowArrayFinishBuildingDefault(array.get(), nullptr);
    ArrowBasicArrayStreamSetArray(&stream, batch, array.get());
  }

  return stream;
}

// ---------------------------------------------------------------------------
// Helper: create a BindStream with ArrowBuffer properly initialized.
// Handle<ArrowBuffer> uses memset(0) which leaves allocator.free as null,
// but ArrowBufferReset (called in destructor) unconditionally calls it.
// ArrowBufferInit sets up the default allocator so destruction is safe.
// ---------------------------------------------------------------------------
static std::unique_ptr<BindStream> MakeSafeBindStream() {
  auto bs = std::make_unique<BindStream>();
  ArrowBufferInit(&bs->param_buffer.value);
  return bs;
}

// ===========================================================================
// Constructor tests
// ===========================================================================

TEST(BindStreamTest, ConstructorDefaults) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  // Must init the ArrowBuffer allocator — Handle<ArrowBuffer> zeroes the struct
  // but ArrowBufferReset (called in destructor) requires a valid allocator.
  ArrowBufferInit(&b.param_buffer.value);

  EXPECT_EQ(b.current_row, -1);
  EXPECT_FALSE(b.has_tz_field);
  EXPECT_FALSE(b.autocommit);
  EXPECT_FALSE(b.has_expected_types);
  EXPECT_EQ(b.bind->release, nullptr);
}

// ===========================================================================
// SetBind tests
// ===========================================================================

TEST(BindStreamTest, SetBindMovesStream) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  auto stream = MakeInt32Stream({1, 2, 3});
  ASSERT_NE(stream.release, nullptr);

  b.SetBind(&stream);
  // Original stream should be moved (release=nullptr)
  EXPECT_EQ(stream.release, nullptr);
  // BindStream should now own the stream
  EXPECT_NE(b.bind->release, nullptr);
}

// ===========================================================================
// Begin tests
// ===========================================================================

TEST(BindStreamTest, BeginWithStructSchema) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  auto stream = MakeInt32Stream({10, 20, 30});
  b.SetBind(&stream);

  auto status = b.Begin([]() { return Status::Ok(); });
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(b.bind_schema->n_children, 1);
  EXPECT_EQ(b.bind_schema_fields.size(), 1u);
}

TEST(BindStreamTest, BeginWithNonStructSchema) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  auto stream = MakeNonStructStream();
  b.SetBind(&stream);

  auto status = b.Begin([]() { return Status::Ok(); });
  EXPECT_FALSE(status.ok());
}

TEST(BindStreamTest, BeginCallbackIsInvoked) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  auto stream = MakeInt32Stream({1});
  b.SetBind(&stream);

  bool called = false;
  auto status = b.Begin([&called]() {
    called = true;
    return Status::Ok();
  });
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(called);
}

TEST(BindStreamTest, BeginCallbackErrorPropagates) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  auto stream = MakeInt32Stream({1});
  b.SetBind(&stream);

  auto status = b.Begin(
      []() { return Status::InvalidState("test callback error"); });
  EXPECT_FALSE(status.ok());
}

// ===========================================================================
// ReconcileWithExpectedTypes tests
// ===========================================================================

TEST(BindStreamTest, ReconcileMatchingParamCount) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  auto stream = MakeInt32Stream({1, 2});
  b.SetBind(&stream);
  b.Begin([]() { return Status::Ok(); });

  // Build a PostgresType with 1 child matching the 1-column schema
  PostgresType root(PostgresTypeId::kRecord);
  root.AppendChild("col0", PostgresType(PostgresTypeId::kInt4));

  auto status = b.ReconcileWithExpectedTypes(root);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(b.has_expected_types);
}

TEST(BindStreamTest, ReconcileMismatchParamCount) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  auto stream = MakeInt32Stream({1});
  b.SetBind(&stream);
  b.Begin([]() { return Status::Ok(); });

  // 2 children vs 1-column schema
  PostgresType root(PostgresTypeId::kRecord);
  root.AppendChild("col0", PostgresType(PostgresTypeId::kInt4));
  root.AppendChild("col1", PostgresType(PostgresTypeId::kText));

  auto status = b.ReconcileWithExpectedTypes(root);
  EXPECT_FALSE(status.ok());
}

TEST(BindStreamTest, ReconcileWithUninitializedSchema) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  // Don't call Begin — schema is not initialized
  PostgresType root(PostgresTypeId::kRecord);

  auto status = b.ReconcileWithExpectedTypes(root);
  EXPECT_FALSE(status.ok());
}

// ===========================================================================
// PullNextArray tests
// ===========================================================================

TEST(BindStreamTest, PullNextArrayReadsArray) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  auto stream = MakeInt32Stream({10, 20, 30});
  b.SetBind(&stream);
  b.Begin([]() { return Status::Ok(); });

  auto status = b.PullNextArray();
  EXPECT_TRUE(status.ok());
  EXPECT_NE(b.current->release, nullptr);
  EXPECT_EQ(b.current->length, 3);
}

TEST(BindStreamTest, PullNextArrayEmptyStream) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  auto stream = MakeEmptyInt32Stream();
  b.SetBind(&stream);
  b.Begin([]() { return Status::Ok(); });

  auto status = b.PullNextArray();
  EXPECT_TRUE(status.ok());
  // Empty stream: no arrays → release should be nullptr
  EXPECT_EQ(b.current->release, nullptr);
}

TEST(BindStreamTest, PullNextArrayMultipleBatches) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  auto stream = MakeMultiBatchStream();
  b.SetBind(&stream);
  b.Begin([]() { return Status::Ok(); });

  // First batch
  auto status1 = b.PullNextArray();
  EXPECT_TRUE(status1.ok());
  EXPECT_NE(b.current->release, nullptr);
  EXPECT_EQ(b.current->length, 2);

  // Second batch
  auto status2 = b.PullNextArray();
  EXPECT_TRUE(status2.ok());
  EXPECT_NE(b.current->release, nullptr);
  EXPECT_EQ(b.current->length, 2);

  // Third batch
  auto status3 = b.PullNextArray();
  EXPECT_TRUE(status3.ok());
  EXPECT_NE(b.current->release, nullptr);
  EXPECT_EQ(b.current->length, 2);

  // Past end
  auto status4 = b.PullNextArray();
  EXPECT_TRUE(status4.ok());
  EXPECT_EQ(b.current->release, nullptr);
}

// ===========================================================================
// EnsureNextRow tests
// ===========================================================================

TEST(BindStreamTest, EnsureNextRowIteratesAllRows) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  auto stream = MakeInt32Stream({100, 200, 300});
  b.SetBind(&stream);
  b.Begin([]() { return Status::Ok(); });

  // Row 0
  auto s1 = b.EnsureNextRow();
  EXPECT_TRUE(s1.ok());
  EXPECT_EQ(b.current_row, 0);

  // Row 1
  auto s2 = b.EnsureNextRow();
  EXPECT_TRUE(s2.ok());
  EXPECT_EQ(b.current_row, 1);

  // Row 2
  auto s3 = b.EnsureNextRow();
  EXPECT_TRUE(s3.ok());
  EXPECT_EQ(b.current_row, 2);

  // Past end
  auto s4 = b.EnsureNextRow();
  EXPECT_TRUE(s4.ok());
  EXPECT_EQ(b.current_row, -1);  // stream ended
}

TEST(BindStreamTest, EnsureNextRowCrossBatch) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  auto stream = MakeMultiBatchStream();  // 3 batches × 2 rows each
  b.SetBind(&stream);
  b.Begin([]() { return Status::Ok(); });

  int total_rows = 0;
  while (true) {
    auto status = b.EnsureNextRow();
    ASSERT_TRUE(status.ok());
    if (b.current_row == -1) break;
    total_rows++;
  }
  EXPECT_EQ(total_rows, 6);  // 3 batches × 2 rows
}

TEST(BindStreamTest, EnsureNextRowEmptyStream) {
  auto bs = MakeSafeBindStream(); auto& b = *bs;
  auto stream = MakeEmptyInt32Stream();
  b.SetBind(&stream);
  b.Begin([]() { return Status::Ok(); });

  auto status = b.EnsureNextRow();
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(b.current_row, -1);
}

}  // namespace adbcpq
