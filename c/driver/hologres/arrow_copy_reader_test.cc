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
#include <vector>

#include <gtest/gtest.h>
#include <lz4.h>
#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow.hpp>
#include <nanoarrow/nanoarrow_ipc.h>
#include <nanoarrow/nanoarrow_ipc.hpp>

#include "arrow_copy_reader.h"

namespace adbchg {

// ---------------------------------------------------------------------------
// Test helper: accesses ArrowCopyReader private members via friend class
// ---------------------------------------------------------------------------
class ArrowCopyReaderTestHelper {
 public:
  static int CallDecodeIpcBlob(ArrowCopyReader& reader, const uint8_t* data, int64_t len,
                               struct ArrowArray* out) {
    return reader.DecodeIpcBlob(data, len, out);
  }

  static int CallDecompressLz4(ArrowCopyReader& reader, const uint8_t* src,
                               int32_t src_len, int32_t decompressed_size) {
    return reader.DecompressLz4(src, src_len, decompressed_size);
  }

  static const std::vector<uint8_t>& GetDecompressBuf(const ArrowCopyReader& reader) {
    return reader.decompress_buf_;
  }

  static bool IsFinished(const ArrowCopyReader& reader) { return reader.is_finished_; }
  static bool IsHeaderRead(const ArrowCopyReader& reader) { return reader.header_read_; }
  static bool IsSchemaSet(const ArrowCopyReader& reader) { return reader.schema_set_; }
  static bool UseLz4(const ArrowCopyReader& reader) { return reader.use_lz4_; }
  static AdbcStatusCode GetStatus(const ArrowCopyReader& reader) {
    return reader.status_;
  }

  // Trampoline wrappers
  static int CallGetSchemaTrampoline(struct ArrowArrayStream* self,
                                     struct ArrowSchema* out) {
    return ArrowCopyReader::GetSchemaTrampoline(self, out);
  }
  static int CallGetNextTrampoline(struct ArrowArrayStream* self,
                                   struct ArrowArray* out) {
    return ArrowCopyReader::GetNextTrampoline(self, out);
  }
  static const char* CallGetLastErrorTrampoline(struct ArrowArrayStream* self) {
    return ArrowCopyReader::GetLastErrorTrampoline(self);
  }
  static void CallReleaseTrampoline(struct ArrowArrayStream* self) {
    ArrowCopyReader::ReleaseTrampoline(self);
  }
};

// ---------------------------------------------------------------------------
// Helper: generate Arrow IPC bytes (Schema + RecordBatch) via nanoarrow writer
// ---------------------------------------------------------------------------
static std::vector<uint8_t> MakeArrowIpcBytes(int32_t num_rows) {
  // Build a simple int32 schema + array
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ArrowSchemaSetTypeStruct(schema.get(), 1);
  ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT32);
  ArrowSchemaSetName(schema->children[0], "col0");

  nanoarrow::UniqueArray array;
  ArrowArrayInitFromSchema(array.get(), schema.get(), nullptr);
  ArrowArrayStartAppending(array.get());
  for (int32_t i = 0; i < num_rows; ++i) {
    ArrowArrayAppendInt(array->children[0], i * 10);
    ArrowArrayFinishElement(array.get());
  }
  ArrowArrayFinishBuildingDefault(array.get(), nullptr);

  // Write schema + record batch via ArrowIpcWriter to a buffer
  struct ArrowBuffer buf;
  ArrowBufferInit(&buf);

  struct ArrowIpcOutputStream output;
  ArrowIpcOutputStreamInitBuffer(&output, &buf);

  nanoarrow::ipc::UniqueWriter writer;
  ArrowIpcWriterInit(writer.get(), &output);

  struct ArrowError error;
  ArrowIpcWriterWriteSchema(writer.get(), schema.get(), &error);

  // Build an ArrowArrayView for the writer
  nanoarrow::UniqueArrayView array_view;
  ArrowArrayViewInitFromSchema(array_view.get(), schema.get(), &error);
  ArrowArrayViewSetArray(array_view.get(), array.get(), &error);
  ArrowIpcWriterWriteArrayView(writer.get(), array_view.get(), &error);

  ArrowIpcWriterReset(writer.get());

  std::vector<uint8_t> result(buf.data, buf.data + buf.size_bytes);
  ArrowBufferReset(&buf);
  return result;
}

// ===========================================================================
// Constructor tests
// ===========================================================================

TEST(ArrowCopyReaderTest, ConstructorDefaults) {
  ArrowCopyReader reader(nullptr, false);
  EXPECT_FALSE(ArrowCopyReaderTestHelper::IsFinished(reader));
  EXPECT_FALSE(ArrowCopyReaderTestHelper::IsHeaderRead(reader));
  EXPECT_FALSE(ArrowCopyReaderTestHelper::IsSchemaSet(reader));
  EXPECT_FALSE(ArrowCopyReaderTestHelper::UseLz4(reader));
  EXPECT_EQ(ArrowCopyReaderTestHelper::GetStatus(reader), ADBC_STATUS_OK);
  EXPECT_EQ(reader.last_error(), nullptr);
}

TEST(ArrowCopyReaderTest, ConstructorWithLz4) {
  ArrowCopyReader reader(nullptr, true);
  EXPECT_TRUE(ArrowCopyReaderTestHelper::UseLz4(reader));
}

// ===========================================================================
// DecodeIpcBlob tests
// ===========================================================================

TEST(ArrowCopyReaderTest, DecodeIpcBlobValidData) {
  ArrowCopyReader reader(nullptr, false);
  auto ipc_bytes = MakeArrowIpcBytes(3);

  struct ArrowArray out;
  out.release = nullptr;
  int rc = ArrowCopyReaderTestHelper::CallDecodeIpcBlob(reader, ipc_bytes.data(),
                                                        ipc_bytes.size(), &out);
  EXPECT_EQ(rc, NANOARROW_OK);
  EXPECT_TRUE(ArrowCopyReaderTestHelper::IsSchemaSet(reader));
  EXPECT_NE(out.release, nullptr);
  EXPECT_EQ(out.length, 3);
  if (out.release) out.release(&out);
}

TEST(ArrowCopyReaderTest, DecodeIpcBlobEmptyBatch) {
  ArrowCopyReader reader(nullptr, false);
  auto ipc_bytes = MakeArrowIpcBytes(0);

  struct ArrowArray out;
  out.release = nullptr;
  int rc = ArrowCopyReaderTestHelper::CallDecodeIpcBlob(reader, ipc_bytes.data(),
                                                        ipc_bytes.size(), &out);
  EXPECT_EQ(rc, NANOARROW_OK);
  EXPECT_TRUE(ArrowCopyReaderTestHelper::IsSchemaSet(reader));
  if (out.release) {
    EXPECT_EQ(out.length, 0);
    out.release(&out);
  }
}

TEST(ArrowCopyReaderTest, DecodeIpcBlobTruncatedData) {
  ArrowCopyReader reader(nullptr, false);
  auto ipc_bytes = MakeArrowIpcBytes(3);

  // Truncate to just 10 bytes — not enough for even a message header
  struct ArrowArray out;
  out.release = nullptr;
  int rc = ArrowCopyReaderTestHelper::CallDecodeIpcBlob(reader, ipc_bytes.data(),
                                                        std::min((size_t)10, ipc_bytes.size()),
                                                        &out);
  // Should either fail or produce no output (release=nullptr)
  if (rc == NANOARROW_OK) {
    EXPECT_EQ(out.release, nullptr);
  }
  if (out.release) out.release(&out);
}

TEST(ArrowCopyReaderTest, DecodeIpcBlobZeroLength) {
  ArrowCopyReader reader(nullptr, false);

  struct ArrowArray out;
  out.release = nullptr;
  int rc = ArrowCopyReaderTestHelper::CallDecodeIpcBlob(reader, nullptr, 0, &out);
  // Zero-length → pos(0) >= len(0) → exits loop → sets release=nullptr
  EXPECT_EQ(rc, NANOARROW_OK);
  EXPECT_EQ(out.release, nullptr);
}

TEST(ArrowCopyReaderTest, DecodeIpcBlobEndOfStreamMarker) {
  // 4 zero bytes is the end-of-stream marker
  uint8_t eos[4] = {0, 0, 0, 0};
  ArrowCopyReader reader(nullptr, false);

  struct ArrowArray out;
  out.release = nullptr;
  int rc =
      ArrowCopyReaderTestHelper::CallDecodeIpcBlob(reader, eos, sizeof(eos), &out);
  EXPECT_EQ(rc, NANOARROW_OK);
  EXPECT_EQ(out.release, nullptr);
}

TEST(ArrowCopyReaderTest, DecodeIpcBlobMultipleCalls) {
  ArrowCopyReader reader(nullptr, false);
  auto ipc_bytes = MakeArrowIpcBytes(5);

  // First call sets schema
  struct ArrowArray out1;
  out1.release = nullptr;
  int rc1 = ArrowCopyReaderTestHelper::CallDecodeIpcBlob(reader, ipc_bytes.data(),
                                                         ipc_bytes.size(), &out1);
  EXPECT_EQ(rc1, NANOARROW_OK);
  EXPECT_TRUE(ArrowCopyReaderTestHelper::IsSchemaSet(reader));

  // Second call with same data should also succeed
  struct ArrowArray out2;
  out2.release = nullptr;
  int rc2 = ArrowCopyReaderTestHelper::CallDecodeIpcBlob(reader, ipc_bytes.data(),
                                                         ipc_bytes.size(), &out2);
  EXPECT_EQ(rc2, NANOARROW_OK);

  if (out1.release) out1.release(&out1);
  if (out2.release) out2.release(&out2);
}

// ===========================================================================
// DecompressLz4 tests
// ===========================================================================

TEST(ArrowCopyReaderTest, DecompressLz4ValidData) {
  // Compress known data
  const char* src = "Hello, Arrow IPC world! This is a test of LZ4 compression.";
  int src_len = static_cast<int>(std::strlen(src));
  int max_compressed = LZ4_compressBound(src_len);
  std::vector<char> compressed(max_compressed);
  int compressed_len =
      LZ4_compress_default(src, compressed.data(), src_len, max_compressed);
  ASSERT_GT(compressed_len, 0);

  ArrowCopyReader reader(nullptr, true);
  int rc = ArrowCopyReaderTestHelper::CallDecompressLz4(
      reader, reinterpret_cast<const uint8_t*>(compressed.data()), compressed_len,
      src_len);
  EXPECT_EQ(rc, NANOARROW_OK);

  auto& buf = ArrowCopyReaderTestHelper::GetDecompressBuf(reader);
  ASSERT_EQ(static_cast<int>(buf.size()), src_len);
  EXPECT_EQ(std::memcmp(buf.data(), src, src_len), 0);
}

TEST(ArrowCopyReaderTest, DecompressLz4CorruptedData) {
  // Random garbage should fail LZ4_decompress_safe
  uint8_t garbage[] = {0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0x01, 0x02};
  ArrowCopyReader reader(nullptr, true);
  int rc = ArrowCopyReaderTestHelper::CallDecompressLz4(reader, garbage, sizeof(garbage),
                                                        1024);
  EXPECT_NE(rc, NANOARROW_OK);
}

TEST(ArrowCopyReaderTest, DecompressLz4WrongDecompressedSize) {
  // Compress valid data but claim wrong decompressed size
  const char* src = "test data for LZ4";
  int src_len = static_cast<int>(std::strlen(src));
  int max_compressed = LZ4_compressBound(src_len);
  std::vector<char> compressed(max_compressed);
  int compressed_len =
      LZ4_compress_default(src, compressed.data(), src_len, max_compressed);
  ASSERT_GT(compressed_len, 0);

  ArrowCopyReader reader(nullptr, true);
  // Claim decompressed size is much smaller than actual
  int rc = ArrowCopyReaderTestHelper::CallDecompressLz4(
      reader, reinterpret_cast<const uint8_t*>(compressed.data()), compressed_len, 5);
  // LZ4_decompress_safe returns negative when output buffer is too small
  EXPECT_NE(rc, NANOARROW_OK);
}

TEST(ArrowCopyReaderTest, DecompressLz4EmptyInput) {
  ArrowCopyReader reader(nullptr, true);
  uint8_t dummy = 0;
  int rc = ArrowCopyReaderTestHelper::CallDecompressLz4(reader, &dummy, 0, 0);
  // LZ4_decompress_safe with src_len=0 and dest_size=0 may return negative
  // (implementation-defined), so we just check it doesn't crash
  (void)rc;
}

// ===========================================================================
// ArrowArrayStream trampoline tests (null-safety)
// ===========================================================================

TEST(ArrowCopyReaderTest, GetSchemaTrampolineNullSelf) {
  struct ArrowSchema out;
  out.release = nullptr;
  int rc = ArrowCopyReaderTestHelper::CallGetSchemaTrampoline(nullptr, &out);
  EXPECT_EQ(rc, EINVAL);
}

TEST(ArrowCopyReaderTest, GetSchemaTrampolineNullPrivateData) {
  struct ArrowArrayStream stream;
  std::memset(&stream, 0, sizeof(stream));
  stream.private_data = nullptr;

  struct ArrowSchema out;
  out.release = nullptr;
  int rc = ArrowCopyReaderTestHelper::CallGetSchemaTrampoline(&stream, &out);
  EXPECT_EQ(rc, EINVAL);
}

TEST(ArrowCopyReaderTest, GetNextTrampolineNullSelf) {
  struct ArrowArray out;
  out.release = nullptr;
  int rc = ArrowCopyReaderTestHelper::CallGetNextTrampoline(nullptr, &out);
  EXPECT_EQ(rc, EINVAL);
}

TEST(ArrowCopyReaderTest, GetNextTrampolineNullPrivateData) {
  struct ArrowArrayStream stream;
  std::memset(&stream, 0, sizeof(stream));
  stream.private_data = nullptr;

  struct ArrowArray out;
  out.release = nullptr;
  int rc = ArrowCopyReaderTestHelper::CallGetNextTrampoline(&stream, &out);
  EXPECT_EQ(rc, EINVAL);
}

TEST(ArrowCopyReaderTest, GetLastErrorTrampolineNullSelf) {
  EXPECT_EQ(ArrowCopyReaderTestHelper::CallGetLastErrorTrampoline(nullptr), nullptr);
}

TEST(ArrowCopyReaderTest, GetLastErrorTrampolineNullPrivateData) {
  struct ArrowArrayStream stream;
  std::memset(&stream, 0, sizeof(stream));
  stream.private_data = nullptr;
  EXPECT_EQ(ArrowCopyReaderTestHelper::CallGetLastErrorTrampoline(&stream), nullptr);
}

TEST(ArrowCopyReaderTest, ReleaseTrampolineNullSelf) {
  // Should not crash
  ArrowCopyReaderTestHelper::CallReleaseTrampoline(nullptr);
}

// ===========================================================================
// Trampoline with expired weak_ptr
// ===========================================================================

TEST(ArrowCopyReaderTest, TrampolinesWithExpiredWeakPtr) {
  auto weak = new std::weak_ptr<ArrowCopyReader>();

  struct ArrowArrayStream stream;
  std::memset(&stream, 0, sizeof(stream));
  stream.private_data = weak;
  stream.release = ArrowCopyReaderTestHelper::CallReleaseTrampoline;

  // weak_ptr is default-constructed → lock() returns nullptr
  struct ArrowSchema schema_out;
  schema_out.release = nullptr;
  EXPECT_EQ(ArrowCopyReaderTestHelper::CallGetSchemaTrampoline(&stream, &schema_out), EINVAL);

  struct ArrowArray array_out;
  array_out.release = nullptr;
  EXPECT_EQ(ArrowCopyReaderTestHelper::CallGetNextTrampoline(&stream, &array_out), EINVAL);

  EXPECT_EQ(ArrowCopyReaderTestHelper::CallGetLastErrorTrampoline(&stream), nullptr);

  // Release cleans up the weak_ptr
  ArrowCopyReaderTestHelper::CallReleaseTrampoline(&stream);
  EXPECT_EQ(stream.private_data, nullptr);
  EXPECT_EQ(stream.release, nullptr);
}

// ===========================================================================
// ErrorFromArrayStream tests
// ===========================================================================

TEST(ArrowCopyReaderTest, ErrorFromArrayStreamNullStream) {
  AdbcStatusCode status;
  EXPECT_EQ(ArrowCopyReader::ErrorFromArrayStream(nullptr, &status), nullptr);
}

TEST(ArrowCopyReaderTest, ErrorFromArrayStreamNullPrivateData) {
  struct ArrowArrayStream stream;
  std::memset(&stream, 0, sizeof(stream));
  stream.private_data = nullptr;

  AdbcStatusCode status;
  EXPECT_EQ(ArrowCopyReader::ErrorFromArrayStream(&stream, &status), nullptr);
}

TEST(ArrowCopyReaderTest, ErrorFromArrayStreamExpiredWeakPtr) {
  auto weak = new std::weak_ptr<ArrowCopyReader>();

  struct ArrowArrayStream stream;
  std::memset(&stream, 0, sizeof(stream));
  stream.private_data = weak;

  AdbcStatusCode status;
  EXPECT_EQ(ArrowCopyReader::ErrorFromArrayStream(&stream, &status), nullptr);

  delete weak;
}

// ===========================================================================
// ExportTo + ErrorFromArrayStream integration (with live shared_ptr)
// ===========================================================================

TEST(ArrowCopyReaderTest, ExportToAndErrorFromArrayStream) {
  auto reader = std::make_shared<ArrowCopyReader>(nullptr, false);

  struct ArrowArrayStream stream;
  std::memset(&stream, 0, sizeof(stream));
  reader->ExportTo(&stream);

  EXPECT_NE(stream.get_schema, nullptr);
  EXPECT_NE(stream.get_next, nullptr);
  EXPECT_NE(stream.get_last_error, nullptr);
  EXPECT_NE(stream.release, nullptr);
  EXPECT_NE(stream.private_data, nullptr);

  // ErrorFromArrayStream should return the reader's error
  AdbcStatusCode status = ADBC_STATUS_OK;
  const struct AdbcError* err = ArrowCopyReader::ErrorFromArrayStream(&stream, &status);
  EXPECT_NE(err, nullptr);
  EXPECT_EQ(status, ADBC_STATUS_OK);  // no error yet

  // Clean up
  stream.release(&stream);
  EXPECT_EQ(stream.private_data, nullptr);
}

// ===========================================================================
// Release tests
// ===========================================================================

TEST(ArrowCopyReaderTest, ReleaseWithoutInit) {
  ArrowCopyReader reader(nullptr, false);
  // Release without any data loaded should be safe
  reader.Release();
}

TEST(ArrowCopyReaderTest, ReleaseMultipleTimes) {
  ArrowCopyReader reader(nullptr, false);
  reader.Release();
  reader.Release();  // Second release should also be safe
}

}  // namespace adbchg
