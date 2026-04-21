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

#pragma once

#include <string>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.hpp>

#include "copy/reader.h"
#include "copy/writer.h"
#include "postgres_type.h"

namespace adbcpq {

// ---------------------------------------------------------------------------
// MockTypeResolver  (extracted from postgres_type_test.cc)
// ---------------------------------------------------------------------------

class MockTypeResolver : public PostgresTypeResolver {
 public:
  ArrowErrorCode Init() {
    auto all_types = PostgresTypeIdAll(false);
    PostgresTypeResolver::Item item;
    item.oid = 0;

    // Insert all the base types
    for (auto type_id : all_types) {
      std::string typreceive = PostgresTyprecv(type_id);
      std::string typname = PostgresTypname(type_id);
      item.oid++;
      item.typname = typname.c_str();
      item.typreceive = typreceive.c_str();
      NANOARROW_RETURN_NOT_OK(Insert(item, nullptr));
    }

    // Insert one of each nested type
    item.oid++;
    item.typname = "_bool";
    item.typreceive = "array_recv";
    item.child_oid = GetOID(PostgresTypeId::kBool);
    NANOARROW_RETURN_NOT_OK(Insert(item, nullptr));

    item.oid++;
    item.typname = "boolrange";
    item.typreceive = "range_recv";
    item.base_oid = GetOID(PostgresTypeId::kBool);
    NANOARROW_RETURN_NOT_OK(Insert(item, nullptr));

    item.oid++;
    item.typname = "custombool";
    item.typreceive = "domain_recv";
    item.base_oid = GetOID(PostgresTypeId::kBool);
    NANOARROW_RETURN_NOT_OK(Insert(item, nullptr));

    item.oid++;
    uint32_t class_oid = item.oid;
    std::vector<std::pair<std::string, uint32_t>> record_fields = {
        {"int4_col", GetOID(PostgresTypeId::kInt4)},
        {"text_col", GetOID(PostgresTypeId::kText)}};
    InsertClass(class_oid, std::move(record_fields));

    item.oid++;
    item.typname = "customrecord";
    item.typreceive = "record_recv";
    item.class_oid = class_oid;

    NANOARROW_RETURN_NOT_OK(Insert(item, nullptr));
    return NANOARROW_OK;
  }
};

// ---------------------------------------------------------------------------
// CopyReaderTester  (extracted from copy_test.cc)
// ---------------------------------------------------------------------------

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
// CopyWriterTester  (new helper for writer round-trip tests)
// ---------------------------------------------------------------------------

class CopyWriterTester {
 public:
  ArrowErrorCode Init(struct ArrowSchema* schema, struct ArrowArray* array,
                      const PostgresTypeResolver& type_resolver,
                      const std::vector<PostgresType>& pg_types,
                      struct ArrowError* error = nullptr) {
    NANOARROW_RETURN_NOT_OK(writer_.Init(schema));
    NANOARROW_RETURN_NOT_OK(writer_.InitFieldWriters(type_resolver, pg_types, error));
    NANOARROW_RETURN_NOT_OK(writer_.SetArray(array));
    return NANOARROW_OK;
  }

  ArrowErrorCode WriteAll(struct ArrowError* error) {
    NANOARROW_RETURN_NOT_OK(writer_.WriteHeader(error));
    int result;
    do {
      result = writer_.WriteRecord(error);
    } while (result == NANOARROW_OK);
    return result;
  }

  const struct ArrowBuffer& WriteBuffer() const { return writer_.WriteBuffer(); }
  void Rewind() { writer_.Rewind(); }

 private:
  PostgresCopyStreamWriter writer_;
};

}  // namespace adbcpq
