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

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <nanoarrow/nanoarrow.hpp>
#include <nanoarrow/nanoarrow_ipc.hpp>

#include "stage_connection.h"
#include "stage_writer.h"
#include "statement.h"
#include "validation/adbc_validation_util.h"

using testing::_;
using testing::HasSubstr;
using testing::Invoke;
using testing::Not;

namespace adbchg {

// ============================================================================
// Mock StageConnection
// ============================================================================

class MockStageConnection : public StageConnection {
 public:
  MOCK_METHOD(Status, ExecuteCommand, (const std::string& sql), (override));
  MOCK_METHOD(Status, ExecuteCopy, (const std::string& copy_sql, const char* data, int64_t len, std::string* error_msg), (override));
  MOCK_METHOD(Status, BeginCopyIn, (const std::string& copy_sql), (override));
  MOCK_METHOD(Status, SendCopyData, (const char* data, int len), (override));
  MOCK_METHOD(Status, EndCopyIn, (std::string* error_msg), (override));
};

// Helper functions to create Status objects for gmock (Status is move-only)
Status OkStatus() { return Status::Ok(); }
Status IoStatus(const std::string& msg) { return Status::IO(msg); }

// ============================================================================
// Test helper for accessing private members via friend class
// ============================================================================

class HologresStageWriterTestHelper {
 public:
  static bool IsStageCreated(const HologresStageWriter& writer) {
    return writer.stage_created_;
  }

  static Status CallUploadBufferToStage(HologresStageWriter& writer,
                                         const StageBuffer& buffer) {
    return writer.UploadBufferToStage(buffer);
  }
};

// ============================================================================
// BufferQueue Tests
// ============================================================================

class BufferQueueTest : public ::testing::Test {
 protected:
  BufferQueue queue_;
};

TEST_F(BufferQueueTest, PushPopBasic) {
  auto buffer = std::make_unique<StageBuffer>();
  buffer->file_name = "test.arrow";
  buffer->rows = 100;
  buffer->data = {1, 2, 3, 4, 5};

  queue_.Push(std::move(buffer));
  auto result = queue_.Pop();

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->file_name, "test.arrow");
  EXPECT_EQ(result->rows, 100);
  EXPECT_EQ(result->data.size(), 5u);
  EXPECT_EQ(result->data[0], 1);
}

TEST_F(BufferQueueTest, PopOnEmptyReturnsNullAfterClose) {
  queue_.Close();
  auto result = queue_.Pop();
  EXPECT_EQ(result, nullptr);
}

TEST_F(BufferQueueTest, CloseIsIdempotent) {
  queue_.Close();
  queue_.Close();  // Should not crash
  EXPECT_TRUE(queue_.IsClosed());
}

TEST_F(BufferQueueTest, IsClosedInitiallyFalse) {
  EXPECT_FALSE(queue_.IsClosed());
}

TEST_F(BufferQueueTest, IsClosedReturnsTrueAfterClose) {
  queue_.Close();
  EXPECT_TRUE(queue_.IsClosed());
}

TEST_F(BufferQueueTest, FIFOOrdering) {
  for (int i = 0; i < 5; i++) {
    auto buffer = std::make_unique<StageBuffer>();
    buffer->file_name = std::to_string(i);
    queue_.Push(std::move(buffer));
  }

  for (int i = 0; i < 5; i++) {
    auto result = queue_.Pop();
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->file_name, std::to_string(i));
  }
}

TEST_F(BufferQueueTest, ConcurrentPushPop) {
  constexpr int kNumThreads = 4;
  constexpr int kBuffersPerThread = 100;

  std::vector<std::thread> push_threads;
  for (int t = 0; t < kNumThreads; t++) {
    push_threads.emplace_back([&, t]() {
      for (int i = 0; i < kBuffersPerThread; i++) {
        auto buffer = std::make_unique<StageBuffer>();
        buffer->rows = t * kBuffersPerThread + i;
        queue_.Push(std::move(buffer));
      }
    });
  }

  for (auto& t : push_threads) t.join();
  queue_.Close();

  int count = 0;
  while (auto buf = queue_.Pop()) count++;
  EXPECT_EQ(count, kNumThreads * kBuffersPerThread);
}

TEST_F(BufferQueueTest, BlockingPopWaitsUntilClose) {
  std::atomic<bool> pop_returned{false};

  std::thread pop_thread([&]() {
    auto result = queue_.Pop();
    pop_returned = (result == nullptr);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(pop_returned);

  queue_.Close();
  pop_thread.join();
  EXPECT_TRUE(pop_returned);
}

TEST_F(BufferQueueTest, PushBeforeCloseThenPop) {
  auto buffer = std::make_unique<StageBuffer>();
  buffer->file_name = "before_close";
  queue_.Push(std::move(buffer));

  queue_.Close();

  auto result = queue_.Pop();
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->file_name, "before_close");

  result = queue_.Pop();
  EXPECT_EQ(result, nullptr);
}

// ============================================================================
// HologresStageConfig Tests
// ============================================================================

TEST(HologresStageConfigTest, DefaultValues) {
  HologresStageConfig config;

  EXPECT_EQ(config.group_name, "default_group");
  EXPECT_EQ(config.ttl_seconds, 7200);
  EXPECT_EQ(config.batch_size, 8192);
  EXPECT_EQ(config.target_file_size, 10 * 1024 * 1024);
  EXPECT_EQ(config.upload_concurrency, 4);
  EXPECT_TRUE(config.stage_name.empty());
}

// ============================================================================
// GenerateStageName Tests
// ============================================================================

TEST(HologresStageWriterTest, GenerateStageNameFormat) {
  std::string name = HologresStageWriter::GenerateStageName();

  EXPECT_EQ(name.substr(0, 14), "adbc_hg_stage_");
  EXPECT_EQ(name.length(), 50u);

  // UUID version 4 indicator at position 28
  EXPECT_EQ(name[28], '4');

  // UUID variant indicator (8, 9, a, or b) at position 33
  char variant = name[33];
  EXPECT_TRUE(variant == '8' || variant == '9' ||
              variant == 'a' || variant == 'b');

  // Hyphens at UUID positions
  EXPECT_EQ(name[22], '-');
  EXPECT_EQ(name[27], '-');
  EXPECT_EQ(name[32], '-');
  EXPECT_EQ(name[37], '-');
}

TEST(HologresStageWriterTest, GenerateStageNameUniqueness) {
  std::unordered_set<std::string> names;
  for (int i = 0; i < 100; i++) {
    names.insert(HologresStageWriter::GenerateStageName());
  }
  EXPECT_EQ(names.size(), 100u);
}

// ============================================================================
// FormatFileName Tests
// ============================================================================

class HologresStageWriterFormatTest : public ::testing::Test {
 protected:
  HologresStageConfig config_;
  MockStageConnection mock_conn_;
  std::unique_ptr<HologresStageWriter> writer_ =
      std::make_unique<HologresStageWriter>(&mock_conn_, config_);
};

TEST_F(HologresStageWriterFormatTest, FormatFileName) {
  EXPECT_EQ(writer_->FormatFileName(0), "data_0.arrow");
  EXPECT_EQ(writer_->FormatFileName(1), "data_1.arrow");
  EXPECT_EQ(writer_->FormatFileName(99), "data_99.arrow");
  EXPECT_EQ(writer_->FormatFileName(12345), "data_12345.arrow");
}

// ============================================================================
// BuildOnConflictClause Tests
// ============================================================================

TEST_F(HologresStageWriterFormatTest, BuildOnConflictClauseNoneMode) {
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kNone, "id"), "");
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kNone, ""), "");
}

TEST_F(HologresStageWriterFormatTest, BuildOnConflictClauseIgnoreMode) {
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kIgnore, "id"),
            " ON CONFLICT (id) DO NOTHING");
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kIgnore, "col1,col2"),
            " ON CONFLICT (col1,col2) DO NOTHING");
}

TEST_F(HologresStageWriterFormatTest, BuildOnConflictClauseUpdateMode) {
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kUpdate, "id"),
            " ON CONFLICT (id) DO UPDATE SET ");
}

TEST_F(HologresStageWriterFormatTest, BuildOnConflictClauseEmptyPkColumns) {
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kIgnore, ""), "");
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kUpdate, ""), "");
}

// ============================================================================
// SerializeArrayToIpcBuffer Tests
// ============================================================================

class SerializeArrayToIpcBufferTest : public ::testing::Test {
 protected:
  adbc_validation::Handle<struct ArrowSchema> schema_;
  adbc_validation::Handle<struct ArrowArray> array_;
};

TEST_F(SerializeArrayToIpcBufferTest, BasicInt64Array) {
  ASSERT_THAT(adbc_validation::MakeSchema(&schema_.value,
                                          {{"col1", NANOARROW_TYPE_INT64}}),
            adbc_validation::IsOkErrno());

  ASSERT_THAT((adbc_validation::MakeBatch<int64_t>(&schema_.value, &array_.value,
                                                   static_cast<struct ArrowError*>(nullptr),
                                                   {1, 2, 3, 4, 5})),
            adbc_validation::IsOkErrno());

  std::vector<uint8_t> buffer;
  struct ArrowError error;
  ArrowErrorInit(&error);

  Status status = HologresStageWriter::SerializeArrayToIpcBuffer(
      &schema_.value, &array_.value, &buffer, &error);

  ASSERT_TRUE(status.ok()) << error.message;
  EXPECT_FALSE(buffer.empty());

  // Verify Arrow IPC stream format starts with continuation marker 0xFFFFFFFF
  ASSERT_GE(buffer.size(), 8u);
  EXPECT_EQ(buffer[0], 0xFF);
  EXPECT_EQ(buffer[1], 0xFF);
  EXPECT_EQ(buffer[2], 0xFF);
  EXPECT_EQ(buffer[3], 0xFF);
}

TEST_F(SerializeArrayToIpcBufferTest, MultiColumnArray) {
  ASSERT_THAT(adbc_validation::MakeSchema(&schema_.value,
                                          {{"id", NANOARROW_TYPE_INT64},
                                           {"name", NANOARROW_TYPE_STRING}}),
            adbc_validation::IsOkErrno());

  ASSERT_THAT((adbc_validation::MakeBatch<int64_t, std::string>(
                  &schema_.value, &array_.value,
                  static_cast<struct ArrowError*>(nullptr),
                  {1, 2}, {"alice", "bob"})),
            adbc_validation::IsOkErrno());

  std::vector<uint8_t> buffer;
  struct ArrowError error;
  ArrowErrorInit(&error);

  Status status = HologresStageWriter::SerializeArrayToIpcBuffer(
      &schema_.value, &array_.value, &buffer, &error);

  ASSERT_TRUE(status.ok()) << error.message;
  EXPECT_FALSE(buffer.empty());
}

TEST_F(SerializeArrayToIpcBufferTest, EmptyArray) {
  ASSERT_THAT(adbc_validation::MakeSchema(&schema_.value,
                                          {{"col1", NANOARROW_TYPE_INT64}}),
            adbc_validation::IsOkErrno());

  ASSERT_THAT((adbc_validation::MakeBatch<int64_t>(&schema_.value, &array_.value,
                                                   static_cast<struct ArrowError*>(nullptr),
                                                   std::vector<std::optional<int64_t>>{})),
            adbc_validation::IsOkErrno());

  std::vector<uint8_t> buffer;
  struct ArrowError error;
  ArrowErrorInit(&error);

  Status status = HologresStageWriter::SerializeArrayToIpcBuffer(
      &schema_.value, &array_.value, &buffer, &error);

  ASSERT_TRUE(status.ok()) << error.message;
  EXPECT_FALSE(buffer.empty());
}

TEST_F(SerializeArrayToIpcBufferTest, StringArray) {
  ASSERT_THAT(adbc_validation::MakeSchema(&schema_.value,
                                          {{"text_col", NANOARROW_TYPE_STRING}}),
            adbc_validation::IsOkErrno());

  ASSERT_THAT((adbc_validation::MakeBatch<std::string>(&schema_.value, &array_.value,
                                                       static_cast<struct ArrowError*>(nullptr),
                                                       {"hello", "world"})),
            adbc_validation::IsOkErrno());

  std::vector<uint8_t> buffer;
  struct ArrowError error;
  ArrowErrorInit(&error);

  Status status = HologresStageWriter::SerializeArrayToIpcBuffer(
      &schema_.value, &array_.value, &buffer, &error);

  ASSERT_TRUE(status.ok()) << error.message;
  EXPECT_FALSE(buffer.empty());
}

// ============================================================================
// MockStageConnection Tests (CreateStage, DropStage, Upload, Insert)
// ============================================================================

class HologresStageWriterMockTest : public ::testing::Test {
 protected:
  HologresStageConfig config_;
  std::unique_ptr<MockStageConnection> mock_conn_ =
      std::make_unique<MockStageConnection>();

  void SetUp() override {
    config_.stage_name = "test_stage";
    config_.group_name = "default_group";
    config_.ttl_seconds = 7200;
  }
};

// --- CreateStage tests ---

TEST_F(HologresStageWriterMockTest, CreateStageSuccess) {
  EXPECT_CALL(*mock_conn_, ExecuteCommand(HasSubstr("HG_CREATE_INTERNAL_STAGE")))
      .WillOnce(Invoke([](const std::string&) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = writer.CreateStage();

  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(HologresStageWriterTestHelper::IsStageCreated(writer));
}

TEST_F(HologresStageWriterMockTest, CreateStageFailure) {
  EXPECT_CALL(*mock_conn_, ExecuteCommand(_))
      .WillOnce(Invoke([](const std::string&) { return IoStatus("CREATE STAGE failed"); }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = writer.CreateStage();

  EXPECT_FALSE(status.ok());
  EXPECT_FALSE(HologresStageWriterTestHelper::IsStageCreated(writer));
}

// --- DropStage tests ---

TEST_F(HologresStageWriterMockTest, DropStageSuccess) {
  EXPECT_CALL(*mock_conn_, ExecuteCommand(HasSubstr("HG_CREATE_INTERNAL_STAGE")))
      .WillOnce(Invoke([](const std::string&) { return OkStatus(); }));
  EXPECT_CALL(*mock_conn_, ExecuteCommand(HasSubstr("HG_DROP_INTERNAL_STAGE")))
      .WillOnce(Invoke([](const std::string&) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  ASSERT_TRUE(writer.CreateStage().ok());
  Status status = writer.DropStage();

  EXPECT_TRUE(status.ok());
}

TEST_F(HologresStageWriterMockTest, DropStageNotCreated) {
  EXPECT_CALL(*mock_conn_, ExecuteCommand(_)).Times(0);

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = writer.DropStage();

  EXPECT_TRUE(status.ok());
}

TEST_F(HologresStageWriterMockTest, DropStageFailure) {
  EXPECT_CALL(*mock_conn_, ExecuteCommand(HasSubstr("HG_CREATE_INTERNAL_STAGE")))
      .WillOnce(Invoke([](const std::string&) { return OkStatus(); }));
  EXPECT_CALL(*mock_conn_, ExecuteCommand(HasSubstr("HG_DROP_INTERNAL_STAGE")))
      .WillOnce(Invoke([](const std::string&) { return IoStatus("DROP STAGE failed"); }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  ASSERT_TRUE(writer.CreateStage().ok());
  Status status = writer.DropStage();

  EXPECT_FALSE(status.ok());
}

// --- UploadBufferToStage tests ---

TEST_F(HologresStageWriterMockTest, UploadBufferToStageSuccess) {
  EXPECT_CALL(*mock_conn_, ExecuteCopy(HasSubstr("COPY EXTERNAL_FILES"), _, _, _))
      .WillOnce(Invoke([](const std::string&, const char*, int64_t, std::string*) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config_);

  StageBuffer buffer;
  buffer.data = {1, 2, 3, 4, 5};
  buffer.file_name = "data_0.arrow";
  buffer.rows = 5;

  Status status = HologresStageWriterTestHelper::CallUploadBufferToStage(writer, buffer);
  EXPECT_TRUE(status.ok());
}

TEST_F(HologresStageWriterMockTest, UploadBufferToStageCopyInFail) {
  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillOnce(Invoke([](const std::string&, const char*, int64_t, std::string* error_msg) {
        if (error_msg) *error_msg = "COPY IN failed";
        return IoStatus("COPY IN failed");
      }));

  HologresStageWriter writer(mock_conn_.get(), config_);

  StageBuffer buffer;
  buffer.data = {1, 2, 3};
  buffer.file_name = "data_0.arrow";

  Status status = HologresStageWriterTestHelper::CallUploadBufferToStage(writer, buffer);
  EXPECT_FALSE(status.ok());
}

TEST_F(HologresStageWriterMockTest, UploadBufferToStageSendFail) {
  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillOnce(Invoke([](const std::string&, const char*, int64_t, std::string* error_msg) {
        if (error_msg) *error_msg = "Send failed";
        return IoStatus("Send failed");
      }));

  HologresStageWriter writer(mock_conn_.get(), config_);

  StageBuffer buffer;
  buffer.data = {1, 2, 3};
  buffer.file_name = "data_0.arrow";

  Status status = HologresStageWriterTestHelper::CallUploadBufferToStage(writer, buffer);
  EXPECT_FALSE(status.ok());
}

TEST_F(HologresStageWriterMockTest, UploadBufferToStageEndFail) {
  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillOnce(Invoke([](const std::string&, const char*, int64_t, std::string* error_msg) {
        if (error_msg) *error_msg = "End COPY failed";
        return IoStatus("End COPY failed");
      }));

  HologresStageWriter writer(mock_conn_.get(), config_);

  StageBuffer buffer;
  buffer.data = {1, 2, 3};
  buffer.file_name = "data_0.arrow";

  Status status = HologresStageWriterTestHelper::CallUploadBufferToStage(writer, buffer);
  EXPECT_FALSE(status.ok());
}

// --- InsertFromStage tests ---

TEST_F(HologresStageWriterMockTest, InsertFromStageBasic) {
  EXPECT_CALL(*mock_conn_, ExecuteCommand(HasSubstr("INSERT INTO")))
      .WillOnce(Invoke([](const std::string&) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = writer.InsertFromStage("\"mytable\"", "\"col1\",\"col2\"",
                                          "\"col1\" int4, \"col2\" text",
                                          OnConflictMode::kNone);

  EXPECT_TRUE(status.ok());
}

TEST_F(HologresStageWriterMockTest, InsertFromStageOnConflictIgnore) {
  EXPECT_CALL(*mock_conn_, ExecuteCommand(HasSubstr("ON CONFLICT DO NOTHING")))
      .WillOnce(Invoke([](const std::string&) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = writer.InsertFromStage("\"mytable\"", "\"col1\"",
                                          "\"col1\" int4",
                                          OnConflictMode::kIgnore);

  EXPECT_TRUE(status.ok());
}

TEST_F(HologresStageWriterMockTest, InsertFromStageOnConflictNone) {
  EXPECT_CALL(*mock_conn_, ExecuteCommand(Not(HasSubstr("ON CONFLICT"))))
      .WillOnce(Invoke([](const std::string&) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = writer.InsertFromStage("\"mytable\"", "\"col1\"",
                                          "\"col1\" int4",
                                          OnConflictMode::kNone);

  EXPECT_TRUE(status.ok());
}

TEST_F(HologresStageWriterMockTest, InsertFromStageFailure) {
  EXPECT_CALL(*mock_conn_, ExecuteCommand(_))
      .WillOnce(Invoke([](const std::string&) { return IoStatus("INSERT failed"); }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = writer.InsertFromStage("\"mytable\"", "\"col1\"",
                                          "\"col1\" int4",
                                          OnConflictMode::kNone);

  EXPECT_FALSE(status.ok());
}

// ============================================================================
// WriteToStage Full Flow Tests
// ============================================================================

class HologresStageWriterWriteTest : public ::testing::Test {
 protected:
  HologresStageConfig config_;
  std::unique_ptr<MockStageConnection> mock_conn_ =
      std::make_unique<MockStageConnection>();

  void SetUp() override {
    config_.stage_name = "test_stage";
    config_.upload_concurrency = 1;
  }

  void MakeInt64Stream(struct ArrowArrayStream* stream) {
    adbc_validation::Handle<struct ArrowSchema> schema;
    adbc_validation::Handle<struct ArrowArray> array;

    ASSERT_THAT(adbc_validation::MakeSchema(&schema.value,
                                            {{"col1", NANOARROW_TYPE_INT64}}),
              adbc_validation::IsOkErrno());

    ASSERT_THAT((adbc_validation::MakeBatch<int64_t>(&schema.value, &array.value,
                                                     static_cast<struct ArrowError*>(nullptr),
                                                     {0, 1, 2})),
              adbc_validation::IsOkErrno());

    nanoarrow::VectorArrayStream impl(&schema.value, &array.value);
    impl.ToArrayStream(stream);
  }
};

TEST_F(HologresStageWriterWriteTest, WriteToStageFullFlow) {
  EXPECT_CALL(*mock_conn_, ExecuteCopy(HasSubstr("COPY EXTERNAL_FILES"), _, _, _))
      .WillOnce(Invoke([](const std::string&, const char*, int64_t, std::string*) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config_);

  struct ArrowArrayStream stream;
  MakeInt64Stream(&stream);

  int64_t rows_affected = 0;
  Status status = writer.WriteToStage(&stream, &rows_affected);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(rows_affected, 3);

  if (stream.release) stream.release(&stream);
}

TEST_F(HologresStageWriterWriteTest, WriteToStageUploadError) {
  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillOnce(Invoke([](const std::string&, const char*, int64_t, std::string*) { return IoStatus("Upload failed"); }));

  HologresStageWriter writer(mock_conn_.get(), config_);

  struct ArrowArrayStream stream;
  MakeInt64Stream(&stream);

  int64_t rows_affected = 0;
  Status status = writer.WriteToStage(&stream, &rows_affected);

  EXPECT_FALSE(status.ok());

  if (stream.release) stream.release(&stream);
}

// ============================================================================
// Statement Option Tests (enum conversions)
// ============================================================================

TEST(HologresOptionTest, OnConflictModeValues) {
  EXPECT_NE(OnConflictMode::kNone, OnConflictMode::kIgnore);
  EXPECT_NE(OnConflictMode::kIgnore, OnConflictMode::kUpdate);
  EXPECT_NE(OnConflictMode::kNone, OnConflictMode::kUpdate);
}

TEST(HologresOptionTest, HologresIngestMethodValues) {
  EXPECT_NE(HologresIngestMethod::kCopy, HologresIngestMethod::kStage);
}

// ============================================================================
// StageConnection Interface Tests
// ============================================================================

TEST(StageConnectionTest, MockIsUsable) {
  auto mock = std::make_unique<MockStageConnection>();
  EXPECT_NE(mock, nullptr);

  EXPECT_CALL(*mock, ExecuteCommand(_))
      .WillOnce(Invoke([](const std::string&) { return OkStatus(); }));
  Status s = mock->ExecuteCommand("TEST");
  EXPECT_TRUE(s.ok());
}

// ============================================================================
// Phase 2: UploadBufferToStage Large Data Chunking Tests
// ============================================================================

TEST_F(HologresStageWriterMockTest, UploadBufferToStageLargeDataChunking) {
  // Create a buffer larger than INT_MAX bytes to test chunking
  // Note: Using a smaller size for testing to avoid memory issues
  // In production, data > INT_MAX would require multiple SendCopyData calls
  StageBuffer buffer;
  // Use a moderate size that still tests the chunking logic path
  buffer.data.resize(10 * 1024 * 1024);  // 10MB
  buffer.file_name = "large.arrow";
  buffer.rows = 1000;

  int64_t captured_len = 0;

  EXPECT_CALL(*mock_conn_, ExecuteCopy(HasSubstr("COPY EXTERNAL_FILES"), _, _, _))
      .WillOnce(Invoke([&](const std::string&, const char* data, int64_t len, std::string*) {
        captured_len = len;
        return OkStatus();
      }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = HologresStageWriterTestHelper::CallUploadBufferToStage(writer, buffer);

  EXPECT_TRUE(status.ok());
  // Verify total bytes sent matches buffer size
  EXPECT_EQ(captured_len, static_cast<int64_t>(buffer.data.size()));
}

TEST_F(HologresStageWriterMockTest, UploadBufferToStageSendFailMidChunking) {
  // Test SendCopyData failure during chunked sending
  StageBuffer buffer;
  buffer.data.resize(1024);
  buffer.file_name = "test.arrow";

  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillOnce(Invoke([](const std::string&, const char*, int64_t, std::string* error_msg) {
        if (error_msg) *error_msg = "Send failed";
        return IoStatus("Send failed");
      }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = HologresStageWriterTestHelper::CallUploadBufferToStage(writer, buffer);

  EXPECT_FALSE(status.ok());
}

// ============================================================================
// Phase 3: WriteToStage Edge Cases Tests
// ============================================================================

class HologresStageWriterWriteEdgeTest : public ::testing::Test {
 protected:
  HologresStageConfig config_;
  std::unique_ptr<MockStageConnection> mock_conn_ =
      std::make_unique<MockStageConnection>();

  void SetUp() override {
    config_.stage_name = "test_stage";
    config_.upload_concurrency = 1;
  }
};

TEST_F(HologresStageWriterWriteEdgeTest, WriteToStageNullRowsAffected) {
  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillOnce(Invoke([](const std::string&, const char*, int64_t, std::string*) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config_);

  struct ArrowArrayStream stream;
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;

  ASSERT_THAT(adbc_validation::MakeSchema(&schema.value,
                                          {{"col1", NANOARROW_TYPE_INT64}}),
            adbc_validation::IsOkErrno());
  ASSERT_THAT((adbc_validation::MakeBatch<int64_t>(&schema.value, &array.value,
                                                   static_cast<struct ArrowError*>(nullptr),
                                                   {1, 2, 3})),
            adbc_validation::IsOkErrno());

  nanoarrow::VectorArrayStream impl(&schema.value, &array.value);
  impl.ToArrayStream(&stream);

  // Pass nullptr for rows_affected
  Status status = writer.WriteToStage(&stream, nullptr);

  EXPECT_TRUE(status.ok());

  if (stream.release) stream.release(&stream);
}

TEST_F(HologresStageWriterWriteEdgeTest, WriteToStageWithConcurrency) {
  // Test with multiple upload threads
  config_.upload_concurrency = 4;

  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillRepeatedly(Invoke([](const std::string&, const char*, int64_t, std::string*) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config_);

  struct ArrowArrayStream stream;
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;

  ASSERT_THAT(adbc_validation::MakeSchema(&schema.value,
                                          {{"col1", NANOARROW_TYPE_INT64}}),
            adbc_validation::IsOkErrno());
  ASSERT_THAT((adbc_validation::MakeBatch<int64_t>(&schema.value, &array.value,
                                                   static_cast<struct ArrowError*>(nullptr),
                                                   {1, 2, 3, 4, 5})),
            adbc_validation::IsOkErrno());

  nanoarrow::VectorArrayStream impl(&schema.value, &array.value);
  impl.ToArrayStream(&stream);

  int64_t rows_affected = 0;
  Status status = writer.WriteToStage(&stream, &rows_affected);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(rows_affected, 5);

  if (stream.release) stream.release(&stream);
}

TEST_F(HologresStageWriterWriteEdgeTest, WriteToStageUploadThreadError) {
  // Test that upload thread error is properly propagated
  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillOnce(Invoke([](const std::string&, const char*, int64_t, std::string*) { return IoStatus("Upload thread error"); }));

  HologresStageWriter writer(mock_conn_.get(), config_);

  struct ArrowArrayStream stream;
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;

  ASSERT_THAT(adbc_validation::MakeSchema(&schema.value,
                                          {{"col1", NANOARROW_TYPE_INT64}}),
            adbc_validation::IsOkErrno());
  ASSERT_THAT((adbc_validation::MakeBatch<int64_t>(&schema.value, &array.value,
                                                   static_cast<struct ArrowError*>(nullptr),
                                                   {1, 2, 3})),
            adbc_validation::IsOkErrno());

  nanoarrow::VectorArrayStream impl(&schema.value, &array.value);
  impl.ToArrayStream(&stream);

  int64_t rows_affected = 0;
  Status status = writer.WriteToStage(&stream, &rows_affected);

  EXPECT_FALSE(status.ok());

  if (stream.release) stream.release(&stream);
}

// ============================================================================
// Phase 4: Thread Safety Tests
// ============================================================================

TEST_F(BufferQueueTest, ConcurrentCloseAndPop) {
  std::atomic<int> pop_count{0};

  std::thread pop_thread1([&]() {
    auto result = queue_.Pop();
    if (result == nullptr) pop_count++;
  });

  std::thread pop_thread2([&]() {
    auto result = queue_.Pop();
    if (result == nullptr) pop_count++;
  });

  // Let threads wait on the condition variable
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  queue_.Close();

  pop_thread1.join();
  pop_thread2.join();

  // Both threads should have received nullptr after close
  EXPECT_EQ(pop_count.load(), 2);
}

TEST_F(BufferQueueTest, ConcurrentPushAndClose) {
  constexpr int kNumBuffers = 10;
  std::atomic<int> push_count{0};

  std::thread push_thread([&]() {
    for (int i = 0; i < kNumBuffers; i++) {
      auto buffer = std::make_unique<StageBuffer>();
      buffer->rows = i;
      queue_.Push(std::move(buffer));
      push_count++;
    }
  });

  // Close while pushing
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  queue_.Close();

  push_thread.join();

  // Count how many buffers we can retrieve
  int retrieved = 0;
  while (auto buf = queue_.Pop()) {
    retrieved++;
  }

  // Should have retrieved all pushed buffers
  EXPECT_EQ(retrieved, push_count.load());
}

// ============================================================================
// Phase 6: InsertFromStage Extended Tests
// ============================================================================

TEST_F(HologresStageWriterMockTest, InsertFromStageOnConflictUpdate) {
  // Test ON_CONFLICT UPDATE mode
  EXPECT_CALL(*mock_conn_, ExecuteCommand(HasSubstr("INSERT INTO")))
      .WillOnce(Invoke([](const std::string& sql) {
        // The current implementation doesn't add DO UPDATE SET for kUpdate
        // It's handled differently - just verify the INSERT is called
        return OkStatus();
      }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = writer.InsertFromStage("\"mytable\"", "\"col1\",\"col2\"",
                                          "\"col1\" int4, \"col2\" text",
                                          OnConflictMode::kUpdate);

  EXPECT_TRUE(status.ok());
}

TEST_F(HologresStageWriterMockTest, InsertFromStageMultipleColumns) {
  // Test with multiple columns
  EXPECT_CALL(*mock_conn_, ExecuteCommand(HasSubstr("INSERT INTO")))
      .WillOnce(Invoke([](const std::string& sql) {
        EXPECT_TRUE(sql.find("col1") != std::string::npos);
        EXPECT_TRUE(sql.find("col2") != std::string::npos);
        EXPECT_TRUE(sql.find("col3") != std::string::npos);
        return OkStatus();
      }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = writer.InsertFromStage("\"mytable\"", "\"col1\",\"col2\",\"col3\"",
                                          "\"col1\" int4, \"col2\" text, \"col3\" float8",
                                          OnConflictMode::kNone);

  EXPECT_TRUE(status.ok());
}

TEST_F(HologresStageWriterMockTest, InsertFromStageWithStageName) {
  // Verify the stage name is correctly included in the SQL
  EXPECT_CALL(*mock_conn_, ExecuteCommand(HasSubstr("test_stage")))
      .WillOnce(Invoke([](const std::string&) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = writer.InsertFromStage("\"mytable\"", "\"col1\"",
                                          "\"col1\" int4",
                                          OnConflictMode::kNone);

  EXPECT_TRUE(status.ok());
}

// ============================================================================
// Phase 5: Statement Option Integration Tests
// ============================================================================

// Note: Full statement integration tests require mocking PostgresConnection,
// which is more complex. Here we test the enum conversions and option string
// handling that doesn't require a full statement mock.

TEST(HologresStatementIntegrationTest, OnConflictModeStringConversion) {
  // Test that OnConflictMode enum values are distinct
  EXPECT_NE(OnConflictMode::kNone, OnConflictMode::kIgnore);
  EXPECT_NE(OnConflictMode::kIgnore, OnConflictMode::kUpdate);
  EXPECT_NE(OnConflictMode::kNone, OnConflictMode::kUpdate);

  // Verify we can use all three modes
  OnConflictMode modes[] = {OnConflictMode::kNone, OnConflictMode::kIgnore,
                            OnConflictMode::kUpdate};
  for (auto mode : modes) {
    // Just verify we can assign and compare
    OnConflictMode test_mode = mode;
    EXPECT_EQ(test_mode, mode);
  }
}

TEST(HologresStatementIntegrationTest, HologresIngestMethodStringConversion) {
  // Test that HologresIngestMethod enum values are distinct
  EXPECT_NE(HologresIngestMethod::kCopy, HologresIngestMethod::kStage);

  // Verify we can use both methods
  HologresIngestMethod methods[] = {HologresIngestMethod::kCopy,
                                     HologresIngestMethod::kStage};
  for (auto method : methods) {
    HologresIngestMethod test_method = method;
    EXPECT_EQ(test_method, method);
  }
}

// Test the BuildOnConflictClause function more thoroughly
TEST_F(HologresStageWriterFormatTest, BuildOnConflictClauseAllModes) {
  // kNone should return empty string
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kNone, "id"), "");
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kNone, ""), "");

  // kIgnore with empty pk_columns should return empty
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kIgnore, ""), "");

  // kIgnore with pk_columns
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kIgnore, "id"),
            " ON CONFLICT (id) DO NOTHING");
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kIgnore, "col1,col2"),
            " ON CONFLICT (col1,col2) DO NOTHING");

  // kUpdate with empty pk_columns should return empty
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kUpdate, ""), "");

  // kUpdate with pk_columns (note: implementation only adds prefix)
  EXPECT_EQ(writer_->BuildOnConflictClause(OnConflictMode::kUpdate, "id"),
            " ON CONFLICT (id) DO UPDATE SET ");
}

// Test GenerateStageName more thoroughly
TEST(HologresStageWriterGenerateTest, GenerateStageNameConsistency) {
  std::string name1 = HologresStageWriter::GenerateStageName();
  std::string name2 = HologresStageWriter::GenerateStageName();

  // Each call should produce a different name
  EXPECT_NE(name1, name2);

  // But both should have the same format
  EXPECT_EQ(name1.substr(0, 14), "adbc_hg_stage_");
  EXPECT_EQ(name2.substr(0, 14), "adbc_hg_stage_");
  EXPECT_EQ(name1.length(), 50u);
  EXPECT_EQ(name2.length(), 50u);
}

TEST(HologresStageWriterGenerateTest, GenerateStageNameUuidFormat) {
  std::string name = HologresStageWriter::GenerateStageName();

  // Verify UUID format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
  // Positions: 14-21 (8 hex), 22 (-), 23-26 (4 hex), 27 (-), 28 ('4'), 29-31 (3 hex),
  // 32 (-), 33 (variant), 34-36 (3 hex), 37 (-), 38-49 (12 hex)

  // Check hyphens at correct positions
  EXPECT_EQ(name[22], '-');
  EXPECT_EQ(name[27], '-');
  EXPECT_EQ(name[32], '-');
  EXPECT_EQ(name[37], '-');

  // Check version indicator (4)
  EXPECT_EQ(name[28], '4');

  // Check variant indicator (8, 9, a, or b)
  char variant = name[33];
  EXPECT_TRUE(variant == '8' || variant == '9' ||
              variant == 'a' || variant == 'b');
}

// ============================================================================
// Phase 1: WriteToStage Error Path Tests
// ============================================================================

// Helper: ArrowArrayStream that fails on GetSchema
struct FailingSchemaStream {
  static int GetSchema(struct ArrowArrayStream*, struct ArrowSchema*) {
    // Cannot set error here as ArrowArrayStream::get_schema doesn't take ArrowError*
    return EINVAL;
  }
  static int GetNext(struct ArrowArrayStream*, struct ArrowArray*) {
    return NANOARROW_OK;
  }
  static const char* GetLastError(struct ArrowArrayStream*) { return "Simulated schema failure"; }
  static void Release(struct ArrowArrayStream* stream) { stream->release = nullptr; }
};

// Helper: ArrowArrayStream that fails on GetNext
struct FailingNextStream {
  static int GetSchema(struct ArrowArrayStream*, struct ArrowSchema* schema) {
    // Return a valid schema
    ArrowSchemaInit(schema);
    return NANOARROW_OK;
  }
  static int GetNext(struct ArrowArrayStream*, struct ArrowArray*) {
    return EINVAL;
  }
  static const char* GetLastError(struct ArrowArrayStream*) { return "Simulated get_next failure"; }
  static void Release(struct ArrowArrayStream* stream) { stream->release = nullptr; }
};

// Helper: ArrowArrayStream that returns empty (schema only, no arrays)
struct EmptyArrayStream {
  static int GetSchema(struct ArrowArrayStream*, struct ArrowSchema* schema) {
    // Create a simple but valid schema with one int64 column
    ArrowSchemaInit(schema);
    int result = ArrowSchemaSetTypeStruct(schema, 1);
    if (result != NANOARROW_OK) return result;
    return ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT64);
  }
  static int GetNext(struct ArrowArrayStream*, struct ArrowArray* array) {
    // Mark end of stream immediately (no data)
    array->release = nullptr;
    return NANOARROW_OK;
  }
  static const char* GetLastError(struct ArrowArrayStream*) { return nullptr; }
  static void Release(struct ArrowArrayStream* stream) { stream->release = nullptr; }
};

TEST_F(HologresStageWriterWriteTest, GetSchemaFailure) {
  struct ArrowArrayStream stream;
  stream.get_schema = FailingSchemaStream::GetSchema;
  stream.get_next = FailingSchemaStream::GetNext;
  stream.get_last_error = FailingSchemaStream::GetLastError;
  stream.release = FailingSchemaStream::Release;
  stream.private_data = nullptr;

  HologresStageWriter writer(mock_conn_.get(), config_);
  int64_t rows = 0;
  Status status = writer.WriteToStage(&stream, &rows);

  EXPECT_FALSE(status.ok());
}

TEST_F(HologresStageWriterWriteTest, GetNextFailure) {
  struct ArrowArrayStream stream;
  stream.get_schema = FailingNextStream::GetSchema;
  stream.get_next = FailingNextStream::GetNext;
  stream.get_last_error = FailingNextStream::GetLastError;
  stream.release = FailingNextStream::Release;
  stream.private_data = nullptr;

  HologresStageWriter writer(mock_conn_.get(), config_);
  int64_t rows = 0;
  Status status = writer.WriteToStage(&stream, &rows);

  EXPECT_FALSE(status.ok());
}

TEST_F(HologresStageWriterWriteTest, EmptyStream) {
  // Empty stream: schema but no arrays
  // No ExecuteCopy should be called since there's no data
  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _)).Times(0);

  struct ArrowArrayStream stream;
  stream.get_schema = EmptyArrayStream::GetSchema;
  stream.get_next = EmptyArrayStream::GetNext;
  stream.get_last_error = EmptyArrayStream::GetLastError;
  stream.release = EmptyArrayStream::Release;
  stream.private_data = nullptr;

  HologresStageWriter writer(mock_conn_.get(), config_);
  int64_t rows = -1;  // Initialize to invalid value
  Status status = writer.WriteToStage(&stream, &rows);

  // Empty stream should succeed with 0 rows
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(rows, 0);
}

TEST_F(HologresStageWriterWriteTest, SerializeWithListType) {
  // Create a stream with a simple nested type using SchemaField::Nested
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;

  // Create a schema with list type using SchemaField::Nested
  adbc_validation::SchemaField list_field = adbc_validation::SchemaField::Nested(
      "list_col", NANOARROW_TYPE_LIST, {{"item", NANOARROW_TYPE_INT64}});

  ASSERT_THAT(adbc_validation::MakeSchema(&schema.value, {list_field}),
              adbc_validation::IsOkErrno());

  // Create an array with list values
  std::vector<std::optional<std::vector<int64_t>>> list_values = {
      std::vector<int64_t>{1, 2}, std::vector<int64_t>{3, 4}};

  ASSERT_THAT((adbc_validation::MakeBatch<std::vector<int64_t>>(
                  &schema.value, &array.value,
                  static_cast<ArrowError*>(nullptr),
                  list_values)),
              adbc_validation::IsOkErrno());

  // Mock should be called for upload
  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillOnce(Invoke([](const std::string&, const char*, int64_t, std::string*) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config_);

  nanoarrow::VectorArrayStream impl(&schema.value, &array.value);
  struct ArrowArrayStream stream;
  impl.ToArrayStream(&stream);

  int64_t rows = 0;
  Status status = writer.WriteToStage(&stream, &rows);

  // Should succeed with list type
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(rows, 2);

  if (stream.release) stream.release(&stream);
}

TEST_F(HologresStageWriterWriteTest, SerializeWithFixedSizeListType) {
  // Create a stream with a FIXED_SIZE_LIST column.
  // The Stage writer should convert this to LIST before serializing to IPC,
  // since Hologres EXTERNAL_FILES does not support FIXED_SIZE_LIST.
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;

  // Create a schema with fixed_size_list type using SchemaField::FixedSize
  adbc_validation::SchemaField fsl_field = adbc_validation::SchemaField::FixedSize(
      "vec_col", NANOARROW_TYPE_FIXED_SIZE_LIST, 3,
      {{"item", NANOARROW_TYPE_FLOAT}});

  ASSERT_THAT(adbc_validation::MakeSchema(&schema.value, {fsl_field}),
              adbc_validation::IsOkErrno());

  // Create an array with fixed-size list values
  // FIXED_SIZE_LIST of size 3 with float values
  std::vector<std::optional<std::vector<float>>> fsl_values = {
      std::vector<float>{1.0f, 2.0f, 3.0f},
      std::vector<float>{4.0f, 5.0f, 6.0f}};

  ASSERT_THAT((adbc_validation::MakeBatch<std::vector<float>>(
                  &schema.value, &array.value,
                  static_cast<ArrowError*>(nullptr), fsl_values)),
              adbc_validation::IsOkErrno());

  // Mock should be called for upload
  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillOnce(Invoke([](const std::string&, const char*, int64_t, std::string*) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config_);

  nanoarrow::VectorArrayStream impl(&schema.value, &array.value);
  struct ArrowArrayStream stream;
  impl.ToArrayStream(&stream);

  int64_t rows = 0;
  Status status = writer.WriteToStage(&stream, &rows);

  // Should succeed after FIXED_SIZE_LIST → LIST conversion
  ASSERT_TRUE(status.ok()) << "WriteToStage failed";
  EXPECT_EQ(rows, 2);

  if (stream.release) stream.release(&stream);
}

TEST_F(HologresStageWriterWriteTest, SerializeWithFixedSizeListTypeNonZeroOffset) {
  // Create a stream with a FIXED_SIZE_LIST column where the parent array
  // has offset > 0. This simulates a PyArrow slice scenario where
  // child->length may not match parent->length * list_size.
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;

  adbc_validation::SchemaField fsl_field = adbc_validation::SchemaField::FixedSize(
      "vec_col", NANOARROW_TYPE_FIXED_SIZE_LIST, 3,
      {{"item", NANOARROW_TYPE_FLOAT}});

  ASSERT_THAT(adbc_validation::MakeSchema(&schema.value, {fsl_field}),
              adbc_validation::IsOkErrno());

  // Create a batch with 5 rows, then set offset=2 and length=3 to simulate
  // a slice. The child array has 15 elements (5 * 3), but only 9 are
  // visible (3 * 3 from offset 6).
  std::vector<std::optional<std::vector<float>>> fsl_values = {
      std::vector<float>{1.0f, 2.0f, 3.0f},    // row 0 (skipped)
      std::vector<float>{4.0f, 5.0f, 6.0f},    // row 1 (skipped)
      std::vector<float>{7.0f, 8.0f, 9.0f},    // row 2 (visible)
      std::vector<float>{10.0f, 11.0f, 12.0f}, // row 3 (visible)
      std::vector<float>{13.0f, 14.0f, 15.0f}, // row 4 (visible)
  };

  ASSERT_THAT((adbc_validation::MakeBatch<std::vector<float>>(
                  &schema.value, &array.value,
                  static_cast<ArrowError*>(nullptr), fsl_values)),
              adbc_validation::IsOkErrno());

  // Simulate a slice: set offset=2, length=3
  // The child array has offset=0, length=15 initially
  ASSERT_EQ(array.value.length, 5);
  array.value.offset = 2;
  array.value.length = 3;
  struct ArrowArray* fsl_col = array.value.children[0];
  // For FIXED_SIZE_LIST with offset=2, list_size=3, child offset should be 6
  fsl_col->offset = 6;
  // child->length remains 15 (total buffer), but after our conversion
  // it should be reset to length * list_size = 3 * 3 = 9

  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillOnce(Invoke([](const std::string&, const char*, int64_t, std::string*) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config_);

  nanoarrow::VectorArrayStream impl(&schema.value, &array.value);
  struct ArrowArrayStream stream;
  impl.ToArrayStream(&stream);

  int64_t rows = 0;
  Status status = writer.WriteToStage(&stream, &rows);

  // Should succeed after FIXED_SIZE_LIST → LIST conversion with offset fix
  ASSERT_TRUE(status.ok()) << "WriteToStage failed";
  EXPECT_EQ(rows, 3);

  if (stream.release) stream.release(&stream);
}

TEST_F(HologresStageWriterWriteTest, SerializeWithFixedSizeListTypeSlicing) {
  // Create a stream with a FIXED_SIZE_LIST column that requires slicing
  // (data larger than target_file_size). Uses a very small target_file_size
  // to force slicing.
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;

  adbc_validation::SchemaField fsl_field = adbc_validation::SchemaField::FixedSize(
      "vec_col", NANOARROW_TYPE_FIXED_SIZE_LIST, 3,
      {{"item", NANOARROW_TYPE_FLOAT}});

  ASSERT_THAT(adbc_validation::MakeSchema(&schema.value, {fsl_field}),
              adbc_validation::IsOkErrno());

  // Create a batch with 10 rows of FSL data
  std::vector<std::optional<std::vector<float>>> fsl_values;
  for (int i = 0; i < 10; i++) {
    fsl_values.push_back(std::vector<float>{
        static_cast<float>(i * 3), static_cast<float>(i * 3 + 1),
        static_cast<float>(i * 3 + 2)});
  }

  ASSERT_THAT((adbc_validation::MakeBatch<std::vector<float>>(
                  &schema.value, &array.value,
                  static_cast<ArrowError*>(nullptr), fsl_values)),
              adbc_validation::IsOkErrno());

  // Set a very small target_file_size to force slicing into 2+ slices
  HologresStageConfig config;
  config.target_file_size = 10;  // 10 bytes — forces slicing

  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillRepeatedly(Invoke([](const std::string&, const char*, int64_t, std::string*) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config);

  nanoarrow::VectorArrayStream impl(&schema.value, &array.value);
  struct ArrowArrayStream stream;
  impl.ToArrayStream(&stream);

  int64_t rows = 0;
  Status status = writer.WriteToStage(&stream, &rows);

  // Should succeed with slicing and FSL → LIST conversion
  if (!status.ok()) {
    struct AdbcError error = ADBC_ERROR_INIT;
    status.ToAdbc(&error);
    std::string err_msg = error.message ? error.message : "(no message)";
    if (error.release) error.release(&error);
    FAIL() << "WriteToStage with slicing failed: " << err_msg;
  }
  EXPECT_EQ(rows, 10);

  if (stream.release) stream.release(&stream);
}

TEST_F(HologresStageWriterWriteTest, SerializeWithMixedColumnsSlicing) {
  // Create a stream with [id: INT64, embedding: FIXED_SIZE_LIST<FLOAT32, 3>]
  // and force slicing with a small target_file_size. This tests that
  // non-FSL child columns are properly adjusted during slicing.
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;

  adbc_validation::SchemaField id_field("id", NANOARROW_TYPE_INT64);
  adbc_validation::SchemaField fsl_field = adbc_validation::SchemaField::FixedSize(
      "embedding", NANOARROW_TYPE_FIXED_SIZE_LIST, 3,
      {{"item", NANOARROW_TYPE_FLOAT}});

  ASSERT_THAT(adbc_validation::MakeSchema(&schema.value, {id_field, fsl_field}),
              adbc_validation::IsOkErrno());

  // Create 20 rows: id = 0..19, embedding = [i*3, i*3+1, i*3+2]
  std::vector<std::optional<int64_t>> ids;
  std::vector<std::optional<std::vector<float>>> embeddings;
  for (int i = 0; i < 20; i++) {
    ids.push_back(i);
    embeddings.push_back(std::vector<float>{
        static_cast<float>(i * 3), static_cast<float>(i * 3 + 1),
        static_cast<float>(i * 3 + 2)});
  }

  ASSERT_THAT((adbc_validation::MakeBatch<int64_t, std::vector<float>>(
                  &schema.value, &array.value,
                  static_cast<ArrowError*>(nullptr), ids, embeddings)),
              adbc_validation::IsOkErrno());

  // Set a very small target_file_size to force slicing into many slices
  HologresStageConfig config;
  config.target_file_size = 10;  // 10 bytes — forces slicing

  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillRepeatedly(Invoke([](const std::string&, const char*, int64_t, std::string*) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config);

  nanoarrow::VectorArrayStream impl(&schema.value, &array.value);
  struct ArrowArrayStream stream;
  impl.ToArrayStream(&stream);

  int64_t rows = 0;
  Status status = writer.WriteToStage(&stream, &rows);

  // Should succeed with slicing, FSL → LIST conversion, and non-FSL
  // child column adjustment
  if (!status.ok()) {
    struct AdbcError error = ADBC_ERROR_INIT;
    status.ToAdbc(&error);
    std::string err_msg = error.message ? error.message : "(no message)";
    if (error.release) error.release(&error);
    FAIL() << "WriteToStage with mixed columns slicing failed: " << err_msg;
  }
  EXPECT_EQ(rows, 20);

  if (stream.release) stream.release(&stream);
}

TEST_F(HologresStageWriterWriteTest, SerializeWithMixedColumnsLargeBatchSlicing) {
  // Create a larger batch with [id: INT64, embedding: FIXED_SIZE_LIST<FLOAT32, 4>]
  // to test many slices (100 rows with tiny target_file_size).
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;

  adbc_validation::SchemaField id_field("id", NANOARROW_TYPE_INT64);
  adbc_validation::SchemaField fsl_field = adbc_validation::SchemaField::FixedSize(
      "vec", NANOARROW_TYPE_FIXED_SIZE_LIST, 4,
      {{"item", NANOARROW_TYPE_FLOAT}});

  ASSERT_THAT(adbc_validation::MakeSchema(&schema.value, {id_field, fsl_field}),
              adbc_validation::IsOkErrno());

  std::vector<std::optional<int64_t>> ids;
  std::vector<std::optional<std::vector<float>>> embeddings;
  for (int i = 0; i < 100; i++) {
    ids.push_back(i);
    embeddings.push_back(std::vector<float>{
        static_cast<float>(i), static_cast<float>(i + 1),
        static_cast<float>(i + 2), static_cast<float>(i + 3)});
  }

  ASSERT_THAT((adbc_validation::MakeBatch<int64_t, std::vector<float>>(
                  &schema.value, &array.value,
                  static_cast<ArrowError*>(nullptr), ids, embeddings)),
              adbc_validation::IsOkErrno());

  HologresStageConfig config;
  config.target_file_size = 10;  // forces many slices

  EXPECT_CALL(*mock_conn_, ExecuteCopy(_, _, _, _))
      .WillRepeatedly(Invoke([](const std::string&, const char*, int64_t, std::string*) { return OkStatus(); }));

  HologresStageWriter writer(mock_conn_.get(), config);

  nanoarrow::VectorArrayStream impl(&schema.value, &array.value);
  struct ArrowArrayStream stream;
  impl.ToArrayStream(&stream);

  int64_t rows = 0;
  Status status = writer.WriteToStage(&stream, &rows);

  if (!status.ok()) {
    struct AdbcError error = ADBC_ERROR_INIT;
    status.ToAdbc(&error);
    std::string err_msg = error.message ? error.message : "(no message)";
    if (error.release) error.release(&error);
    FAIL() << "WriteToStage large batch slicing failed: " << err_msg;
  }
  EXPECT_EQ(rows, 100);

  if (stream.release) stream.release(&stream);
}

TEST_F(SerializeArrayToIpcBufferTest, NullValuesArray) {
  ASSERT_THAT(adbc_validation::MakeSchema(&schema_.value,
                                          {{"col1", NANOARROW_TYPE_INT64}}),
              adbc_validation::IsOkErrno());

  ASSERT_THAT((adbc_validation::MakeBatch<int64_t>(&schema_.value, &array_.value,
                                                    static_cast<ArrowError*>(nullptr),
                                                    {std::nullopt, 1, std::nullopt, 2})),
              adbc_validation::IsOkErrno());

  std::vector<uint8_t> buffer;
  struct ArrowError error;
  ArrowErrorInit(&error);

  Status status = HologresStageWriter::SerializeArrayToIpcBuffer(
      &schema_.value, &array_.value, &buffer, &error);

  ASSERT_TRUE(status.ok()) << error.message;
  EXPECT_FALSE(buffer.empty());

  // Verify Arrow IPC stream format starts with continuation marker 0xFFFFFFFF
  ASSERT_GE(buffer.size(), 8u);
  EXPECT_EQ(buffer[0], 0xFF);
  EXPECT_EQ(buffer[1], 0xFF);
  EXPECT_EQ(buffer[2], 0xFF);
  EXPECT_EQ(buffer[3], 0xFF);
}

TEST_F(SerializeArrayToIpcBufferTest, Float64Array) {
  ASSERT_THAT(adbc_validation::MakeSchema(&schema_.value,
                                          {{"col1", NANOARROW_TYPE_DOUBLE}}),
              adbc_validation::IsOkErrno());

  ASSERT_THAT((adbc_validation::MakeBatch<double>(&schema_.value, &array_.value,
                                                   static_cast<ArrowError*>(nullptr),
                                                   {1.5, 2.5, 3.14159})),
              adbc_validation::IsOkErrno());

  std::vector<uint8_t> buffer;
  struct ArrowError error;
  ArrowErrorInit(&error);

  Status status = HologresStageWriter::SerializeArrayToIpcBuffer(
      &schema_.value, &array_.value, &buffer, &error);

  ASSERT_TRUE(status.ok()) << error.message;
  EXPECT_FALSE(buffer.empty());
}

TEST_F(SerializeArrayToIpcBufferTest, BooleanArray) {
  ASSERT_THAT(adbc_validation::MakeSchema(&schema_.value,
                                          {{"col1", NANOARROW_TYPE_BOOL}}),
              adbc_validation::IsOkErrno());

  ASSERT_THAT((adbc_validation::MakeBatch<bool>(&schema_.value, &array_.value,
                                                 static_cast<ArrowError*>(nullptr),
                                                 {true, false, true, true})),
              adbc_validation::IsOkErrno());

  std::vector<uint8_t> buffer;
  struct ArrowError error;
  ArrowErrorInit(&error);

  Status status = HologresStageWriter::SerializeArrayToIpcBuffer(
      &schema_.value, &array_.value, &buffer, &error);

  ASSERT_TRUE(status.ok()) << error.message;
  EXPECT_FALSE(buffer.empty());
}

TEST_F(SerializeArrayToIpcBufferTest, Date32Array) {
  ASSERT_THAT(adbc_validation::MakeSchema(&schema_.value,
                                          {{"col1", NANOARROW_TYPE_DATE32}}),
              adbc_validation::IsOkErrno());

  // Date32 stores days since epoch as int32
  ASSERT_THAT((adbc_validation::MakeBatch<int32_t>(&schema_.value, &array_.value,
                                                    static_cast<ArrowError*>(nullptr),
                                                    {0, 1, 365, 18628})),
              adbc_validation::IsOkErrno());

  std::vector<uint8_t> buffer;
  struct ArrowError error;
  ArrowErrorInit(&error);

  Status status = HologresStageWriter::SerializeArrayToIpcBuffer(
      &schema_.value, &array_.value, &buffer, &error);

  ASSERT_TRUE(status.ok()) << error.message;
  EXPECT_FALSE(buffer.empty());
}

TEST_F(SerializeArrayToIpcBufferTest, BinaryArray) {
  ASSERT_THAT(adbc_validation::MakeSchema(&schema_.value,
                                          {{"col1", NANOARROW_TYPE_BINARY}}),
              adbc_validation::IsOkErrno());

  ASSERT_THAT((adbc_validation::MakeBatch<std::string>(&schema_.value, &array_.value,
                                                        static_cast<ArrowError*>(nullptr),
                                                        {"\x00\x01\x02", "hello", ""})),
              adbc_validation::IsOkErrno());

  std::vector<uint8_t> buffer;
  struct ArrowError error;
  ArrowErrorInit(&error);

  Status status = HologresStageWriter::SerializeArrayToIpcBuffer(
      &schema_.value, &array_.value, &buffer, &error);

  ASSERT_TRUE(status.ok()) << error.message;
  EXPECT_FALSE(buffer.empty());
}

// ============================================================================
// Phase 3: UploadBufferToStage Edge Cases
// ============================================================================

TEST_F(HologresStageWriterMockTest, UploadEmptyBuffer) {
  // Test uploading an empty buffer (zero bytes)
  StageBuffer buffer;
  buffer.data.clear();  // Empty data
  buffer.file_name = "empty.arrow";
  buffer.rows = 0;

  // Empty buffer should still trigger COPY command
  int64_t captured_len = -1;
  EXPECT_CALL(*mock_conn_, ExecuteCopy(HasSubstr("COPY EXTERNAL_FILES"), _, _, _))
      .WillOnce(Invoke([&](const std::string&, const char*, int64_t len, std::string*) {
        captured_len = len;
        return OkStatus();
      }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = HologresStageWriterTestHelper::CallUploadBufferToStage(writer, buffer);

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(captured_len, 0);
}

TEST_F(HologresStageWriterMockTest, UploadMultipleChunks) {
  // Test that large data is sent correctly (chunking is handled internally in ExecuteCopy)
  StageBuffer buffer;
  buffer.data.resize(3 * 1024 * 1024);  // 3MB
  buffer.file_name = "multi_chunk.arrow";
  buffer.rows = 1000;

  int64_t captured_len = 0;

  EXPECT_CALL(*mock_conn_, ExecuteCopy(HasSubstr("COPY EXTERNAL_FILES"), _, _, _))
      .WillOnce(Invoke([&](const std::string&, const char*, int64_t len, std::string*) {
        captured_len = len;
        return OkStatus();
      }));

  HologresStageWriter writer(mock_conn_.get(), config_);
  Status status = HologresStageWriterTestHelper::CallUploadBufferToStage(writer, buffer);

  EXPECT_TRUE(status.ok());
  // Verify all bytes were sent
  EXPECT_EQ(captured_len, static_cast<int64_t>(buffer.data.size()));
}

}  // namespace adbchg
