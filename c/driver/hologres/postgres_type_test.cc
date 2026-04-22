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

#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.hpp>

#include "postgres_type.h"
#include "test_util.h"

namespace adbcpq {

TEST(PostgresTypeTest, PostgresTypeBasic) {
  PostgresType type(PostgresTypeId::kBool);
  EXPECT_EQ(type.field_name(), "");
  EXPECT_EQ(type.typname(), "");
  EXPECT_EQ(type.type_id(), PostgresTypeId::kBool);
  EXPECT_EQ(type.oid(), 0);
  EXPECT_EQ(type.n_children(), 0);

  PostgresType with_info = type.WithPgTypeInfo(1234, "some_typename");
  EXPECT_EQ(with_info.oid(), 1234);
  EXPECT_EQ(with_info.typname(), "some_typename");
  EXPECT_EQ(with_info.type_id(), type.type_id());

  PostgresType with_name = type.WithFieldName("some name");
  EXPECT_EQ(with_name.field_name(), "some name");
  EXPECT_EQ(with_name.oid(), type.oid());
  EXPECT_EQ(with_name.type_id(), type.type_id());

  PostgresType array = type.Array(12345, "array type name");
  EXPECT_EQ(array.oid(), 12345);
  EXPECT_EQ(array.typname(), "array type name");
  EXPECT_EQ(array.n_children(), 1);
  EXPECT_EQ(array.child(0).oid(), type.oid());
  EXPECT_EQ(array.child(0).type_id(), type.type_id());

  PostgresType range = type.Range(12345, "range type name");
  EXPECT_EQ(range.oid(), 12345);
  EXPECT_EQ(range.typname(), "range type name");
  EXPECT_EQ(range.n_children(), 1);
  EXPECT_EQ(range.child(0).oid(), type.oid());
  EXPECT_EQ(range.child(0).type_id(), type.type_id());

  PostgresType domain = type.Domain(123456, "domain type name");
  EXPECT_EQ(domain.oid(), 123456);
  EXPECT_EQ(domain.typname(), "domain type name");
  EXPECT_EQ(domain.type_id(), type.type_id());

  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col1", type);
  EXPECT_EQ(record.type_id(), PostgresTypeId::kRecord);
  EXPECT_EQ(record.n_children(), 1);
  EXPECT_EQ(record.child(0).type_id(), type.type_id());
  EXPECT_EQ(record.child(0).field_name(), "col1");
}

TEST(PostgresTypeTest, PostgresTypeSetSchema) {
  nanoarrow::UniqueSchema schema;
  ArrowStringView typnameMetadataValue = ArrowCharView("<not found>");

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kBool).SetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->format, "b");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kInt2).SetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->format, "s");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kInt4).SetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->format, "i");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kInt8).SetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->format, "l");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kFloat4).SetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->format, "f");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kFloat8).SetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->format, "g");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kText).SetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->format, "u");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kBytea).SetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->format, "z");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kNumeric).SetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->format, "u");
  typnameMetadataValue = ArrowCharView("<not found>");
  ArrowMetadataGetValue(schema->metadata, ArrowCharView("ADBC:postgresql:typname"),
                        &typnameMetadataValue);
  EXPECT_EQ(std::string(typnameMetadataValue.data, typnameMetadataValue.size_bytes),
            "numeric");
  ArrowMetadataGetValue(schema->metadata, ArrowCharView("ARROW:extension:name"),
                        &typnameMetadataValue);
  EXPECT_EQ(std::string(typnameMetadataValue.data, typnameMetadataValue.size_bytes),
            "arrow.opaque");
  ArrowMetadataGetValue(schema->metadata, ArrowCharView("ARROW:extension:metadata"),
                        &typnameMetadataValue);
  EXPECT_EQ(std::string(typnameMetadataValue.data, typnameMetadataValue.size_bytes),
            R"({"type_name": "numeric", "vendor_name": "PostgreSQL"})");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kDate).SetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->format, "tdD");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kTimestamp).SetSchema(schema.get()),
            NANOARROW_OK);
  EXPECT_STREQ(schema->format, "tsu:");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kTimestamptz).SetSchema(schema.get()),
            NANOARROW_OK);
  EXPECT_STREQ(schema->format, "tsu:UTC");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kInterval).SetSchema(schema.get()),
            NANOARROW_OK);
  EXPECT_STREQ(schema->format, "tin");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kNumeric).Array().SetSchema(schema.get()),
            NANOARROW_OK);
  EXPECT_STREQ(schema->format, "+l");
  EXPECT_STREQ(schema->children[0]->format, "u");
  typnameMetadataValue = ArrowCharView("<not found>");
  ArrowMetadataGetValue(schema->children[0]->metadata,
                        ArrowCharView("ADBC:postgresql:typname"), &typnameMetadataValue);
  EXPECT_EQ(std::string(typnameMetadataValue.data, typnameMetadataValue.size_bytes),
            "numeric");
  schema.reset();

  ArrowSchemaInit(schema.get());
  EXPECT_EQ(PostgresType(PostgresTypeId::kBool).Array().SetSchema(schema.get()),
            NANOARROW_OK);
  EXPECT_STREQ(schema->format, "+l");
  EXPECT_STREQ(schema->children[0]->format, "b");
  schema.reset();

  ArrowSchemaInit(schema.get());
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("col1", PostgresType(PostgresTypeId::kBool));
  EXPECT_EQ(record.SetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->format, "+s");
  EXPECT_STREQ(schema->children[0]->format, "b");
  schema.reset();

  ArrowSchemaInit(schema.get());
  PostgresType unknown(PostgresTypeId::kBrinMinmaxMultiSummary);
  EXPECT_EQ(unknown.WithPgTypeInfo(0, "some_name").SetSchema(schema.get()), NANOARROW_OK);
  EXPECT_STREQ(schema->format, "z");
  typnameMetadataValue = ArrowCharView("<not found>");
  ArrowMetadataGetValue(schema->metadata, ArrowCharView("ADBC:postgresql:typname"),
                        &typnameMetadataValue);
  EXPECT_EQ(std::string(typnameMetadataValue.data, typnameMetadataValue.size_bytes),
            "some_name");
  schema.reset();
}

TEST(PostgresTypeTest, PostgresTypeFromSchema) {
  nanoarrow::UniqueSchema schema;
  PostgresType type;
  MockTypeResolver resolver;
  ASSERT_EQ(resolver.Init(), NANOARROW_OK);

  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_BOOL), NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kBool);
  schema.reset();

  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INT8), NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kInt2);
  schema.reset();

  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_UINT8), NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kInt2);
  schema.reset();

  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INT16), NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kInt2);
  schema.reset();

  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_UINT16), NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kInt4);
  schema.reset();

  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INT32), NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kInt4);
  schema.reset();

  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_UINT32), NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kInt8);
  schema.reset();

  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INT64), NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kInt8);
  schema.reset();

  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_FLOAT), NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kFloat4);
  schema.reset();

  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_DOUBLE), NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kFloat8);
  schema.reset();

  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_BINARY), NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kBytea);
  schema.reset();

  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_STRING), NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kText);
  schema.reset();

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeDateTime(schema.get(), NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_MICRO, ""),
            NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kTimestamp);
  schema.reset();

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeDateTime(schema.get(), NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_MICRO, "America/Phoenix"),
            NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kTimestamptz);
  schema.reset();

  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetType(schema.get(), NANOARROW_TYPE_LIST), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_BOOL), NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kArray);
  EXPECT_EQ(type.child(0).type_id(), PostgresTypeId::kBool);
  schema.reset();

  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INT64), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateDictionary(schema.get()), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaInitFromType(schema->dictionary, NANOARROW_TYPE_STRING),
            NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kText);
  schema.reset();

  ArrowError error;
  ASSERT_EQ(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INTERVAL_MONTHS),
            NANOARROW_OK);
  EXPECT_EQ(PostgresType::FromSchema(resolver, schema.get(), &type, &error), ENOTSUP);
  EXPECT_STREQ(error.message, "Can't map Arrow type 'interval_months' to Postgres type");
  schema.reset();
}

TEST(PostgresTypeTest, PostgresTypeResolver) {
  PostgresTypeResolver resolver;
  ArrowError error;
  PostgresType type;
  PostgresTypeResolver::Item item;

  // Check error for type not found
  EXPECT_EQ(resolver.Find(123, &type, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Postgres type with oid 123 not found");

  EXPECT_EQ(resolver.FindWithDefault(123, &type), NANOARROW_OK);
  EXPECT_EQ(type.oid(), 123);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kUnnamedArrowOpaque);
  EXPECT_EQ(type.typname(), "unnamed<oid:123>");

  // Check error for Array with unknown child
  item.oid = 123;
  item.typname = "some_array";
  item.typreceive = "array_recv";
  item.child_oid = 1234;
  EXPECT_EQ(resolver.Insert(item, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Postgres type with oid 1234 not found");

  // Check error for Range with unknown child
  item.typname = "some_range";
  item.typreceive = "range_recv";
  item.base_oid = 12345;
  EXPECT_EQ(resolver.Insert(item, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Postgres type with oid 12345 not found");

  // Check error for Domain with unknown child
  item.typname = "some_domain";
  item.typreceive = "domain_recv";
  item.base_oid = 123456;
  EXPECT_EQ(resolver.Insert(item, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Postgres type with oid 123456 not found");

  // Check error for Record with unknown class
  item.typname = "some_record";
  item.typreceive = "record_recv";
  item.class_oid = 123456;
  EXPECT_EQ(resolver.Insert(item, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Class definition with oid 123456 not found");

  // Check insert/resolve of regular type
  item.typname = "some_type_name";
  item.typreceive = "boolrecv";
  item.oid = 10;
  EXPECT_EQ(resolver.Insert(item, &error), NANOARROW_OK);
  EXPECT_EQ(resolver.Find(10, &type, &error), NANOARROW_OK);
  EXPECT_EQ(type.oid(), 10);
  EXPECT_EQ(type.typname(), "some_type_name");
  EXPECT_EQ(type.type_id(), PostgresTypeId::kBool);

  // Check insert/resolve of array type
  item.oid = 11;
  item.typname = "some_array_type_name";
  item.typreceive = "array_recv";
  item.child_oid = 10;
  EXPECT_EQ(resolver.Insert(item, &error), NANOARROW_OK);
  EXPECT_EQ(resolver.Find(11, &type, &error), NANOARROW_OK);
  EXPECT_EQ(type.oid(), 11);
  EXPECT_EQ(type.typname(), "some_array_type_name");
  EXPECT_EQ(type.type_id(), PostgresTypeId::kArray);
  EXPECT_EQ(type.child(0).oid(), 10);
  EXPECT_EQ(type.child(0).type_id(), PostgresTypeId::kBool);

  // Check reverse lookup of array type from item type
  EXPECT_EQ(resolver.FindArray(10, &type, &error), NANOARROW_OK);
  EXPECT_EQ(type.oid(), 11);

  // Check insert/resolve of range type
  item.oid = 12;
  item.typname = "some_range_type_name";
  item.typreceive = "range_recv";
  item.base_oid = 10;
  EXPECT_EQ(resolver.Insert(item, &error), NANOARROW_OK);
  EXPECT_EQ(resolver.Find(12, &type, &error), NANOARROW_OK);
  EXPECT_EQ(type.oid(), 12);
  EXPECT_EQ(type.typname(), "some_range_type_name");
  EXPECT_EQ(type.type_id(), PostgresTypeId::kRange);
  EXPECT_EQ(type.child(0).oid(), 10);
  EXPECT_EQ(type.child(0).type_id(), PostgresTypeId::kBool);

  // Check insert/resolve of domain type
  item.oid = 13;
  item.typname = "some_domain_type_name";
  item.typreceive = "domain_recv";
  item.base_oid = 10;
  EXPECT_EQ(resolver.Insert(item, &error), NANOARROW_OK);
  EXPECT_EQ(resolver.Find(13, &type, &error), NANOARROW_OK);
  EXPECT_EQ(type.oid(), 13);
  EXPECT_EQ(type.typname(), "some_domain_type_name");
  EXPECT_EQ(type.type_id(), PostgresTypeId::kBool);
}

TEST(PostgresTypeTest, PostgresTypeResolveRecord) {
  MockTypeResolver resolver;
  ASSERT_EQ(resolver.Init(), NANOARROW_OK);

  PostgresType type;
  EXPECT_EQ(resolver.Find(resolver.GetOID(PostgresTypeId::kRecord), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.oid(), resolver.GetOID(PostgresTypeId::kRecord));
  EXPECT_EQ(type.n_children(), 2);
  EXPECT_EQ(type.child(0).field_name(), "int4_col");
  EXPECT_EQ(type.child(0).type_id(), PostgresTypeId::kInt4);
  EXPECT_EQ(type.child(1).field_name(), "text_col");
  EXPECT_EQ(type.child(1).type_id(), PostgresTypeId::kText);
}

TEST(PostgresTypeTest, PostgresTypeResolveInt2vector) {
  MockTypeResolver resolver;
  ASSERT_EQ(resolver.Init(), NANOARROW_OK);

  PostgresType type;

  const auto int2vector_oid = resolver.GetOID(PostgresTypeId::kInt2vector);
  EXPECT_EQ(resolver.Find(int2vector_oid, &type, nullptr), NANOARROW_OK);
  EXPECT_EQ(type.oid(), int2vector_oid);
  EXPECT_EQ(type.typname(), "int2vector");
  EXPECT_EQ(type.type_id(), PostgresTypeId::kInt2vector);
  EXPECT_EQ(0, type.n_children());
}

TEST(PostgresTypeTest, SqlTypeName) {
  PostgresType int4(PostgresTypeId::kInt4);
  int4 = int4.WithPgTypeInfo(0, "int4");
  EXPECT_EQ(int4.sql_type_name(), "int4");

  PostgresType array = int4.Array(0, "_int4");
  EXPECT_EQ(array.sql_type_name(), "int4 ARRAY");
}

TEST(PostgresTypeTest, DefaultConstructor) {
  PostgresType type;
  EXPECT_EQ(type.type_id(), PostgresTypeId::kUninitialized);
  EXPECT_EQ(type.oid(), 0);
  EXPECT_EQ(type.typname(), "");
  EXPECT_EQ(type.field_name(), "");
  EXPECT_EQ(type.n_children(), 0);
}

TEST(PostgresTypeTest, UnnamedType) {
  PostgresType type = PostgresType::Unnamed(42);
  EXPECT_EQ(type.type_id(), PostgresTypeId::kUnnamedArrowOpaque);
  EXPECT_EQ(type.oid(), 42);
  EXPECT_EQ(type.typname(), "unnamed<oid:42>");
}

TEST(PostgresTypeTest, GetOIDNotFound) {
  PostgresTypeResolver resolver;
  EXPECT_EQ(resolver.GetOID(PostgresTypeId::kBool), 0);
}

TEST(PostgresTypeTest, FindArrayNotFound) {
  PostgresTypeResolver resolver;
  PostgresType type;
  ArrowError error;
  EXPECT_EQ(resolver.FindArray(999, &type, &error), EINVAL);
}

// ---------------------------------------------------------------------------
// SetSchema: record (struct) type
// ---------------------------------------------------------------------------

TEST(PostgresTypeTest, SetSchemaRecord) {
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("a", PostgresType(PostgresTypeId::kInt4));
  record.AppendChild("b", PostgresType(PostgresTypeId::kText));

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(record.SetSchema(schema.get()), NANOARROW_OK);

  EXPECT_STREQ(schema->format, "+s");
  ASSERT_EQ(schema->n_children, 2);
  EXPECT_STREQ(schema->children[0]->format, "i");  // INT32
  EXPECT_STREQ(schema->children[0]->name, "a");
  EXPECT_STREQ(schema->children[1]->format, "u");  // STRING
  EXPECT_STREQ(schema->children[1]->name, "b");
}

// ---------------------------------------------------------------------------
// SetSchema: array (list) type
// ---------------------------------------------------------------------------

TEST(PostgresTypeTest, SetSchemaArray) {
  PostgresType arr = PostgresType(PostgresTypeId::kInt4).Array(0, "_int4");

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(arr.SetSchema(schema.get()), NANOARROW_OK);

  EXPECT_STREQ(schema->format, "+l");  // LIST
  ASSERT_EQ(schema->n_children, 1);
  EXPECT_STREQ(schema->children[0]->format, "i");  // INT32 child
  EXPECT_STREQ(schema->children[0]->name, "item");
}

// ---------------------------------------------------------------------------
// SetSchema: timestamptz with UTC timezone
// ---------------------------------------------------------------------------

TEST(PostgresTypeTest, SetSchemaTimestamptz) {
  PostgresType ts(PostgresTypeId::kTimestamptz);

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ts.SetSchema(schema.get()), NANOARROW_OK);

  EXPECT_STREQ(schema->format, "tsu:UTC");
}

// ---------------------------------------------------------------------------
// SetSchema: interval
// ---------------------------------------------------------------------------

TEST(PostgresTypeTest, SetSchemaInterval) {
  PostgresType interval(PostgresTypeId::kInterval);

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(interval.SetSchema(schema.get()), NANOARROW_OK);

  EXPECT_STREQ(schema->format, "tin");
}

// ---------------------------------------------------------------------------
// SetSchema: user-defined type → BINARY with metadata
// ---------------------------------------------------------------------------

TEST(PostgresTypeTest, SetSchemaUserDefined) {
  PostgresType ud =
      PostgresType(PostgresTypeId::kUserDefined).WithPgTypeInfo(12345, "mytype");

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ud.SetSchema(schema.get()), NANOARROW_OK);

  EXPECT_STREQ(schema->format, "z");  // BINARY
}

// ---------------------------------------------------------------------------
// SetSchema: int2vector → LIST of INT16
// ---------------------------------------------------------------------------

TEST(PostgresTypeTest, SetSchemaInt2vector) {
  PostgresType iv(PostgresTypeId::kInt2vector);

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(iv.SetSchema(schema.get()), NANOARROW_OK);

  EXPECT_STREQ(schema->format, "+l");  // LIST
  ASSERT_EQ(schema->n_children, 1);
  EXPECT_STREQ(schema->children[0]->format, "s");  // INT16
}

// ---------------------------------------------------------------------------
// sql_type_name()
// ---------------------------------------------------------------------------

TEST(PostgresTypeTest, SqlTypeNameScalar) {
  PostgresType t = PostgresType(PostgresTypeId::kInt4).WithPgTypeInfo(23, "int4");
  EXPECT_EQ(t.sql_type_name(), "int4");
}

TEST(PostgresTypeTest, SqlTypeNameArray) {
  PostgresType arr =
      PostgresType(PostgresTypeId::kInt4).WithPgTypeInfo(23, "int4").Array(1007, "_int4");
  EXPECT_EQ(arr.sql_type_name(), "int4 ARRAY");
}

// ---------------------------------------------------------------------------
// Unnamed() static factory
// ---------------------------------------------------------------------------

TEST(PostgresTypeTest, UnnamedTypeLargeOid) {
  PostgresType t = PostgresType::Unnamed(99999);
  EXPECT_EQ(t.type_id(), PostgresTypeId::kUnnamedArrowOpaque);
  EXPECT_EQ(t.oid(), 99999u);
  EXPECT_EQ(t.typname(), "unnamed<oid:99999>");
}

// ---------------------------------------------------------------------------
// FromSchema: various Arrow types
// ---------------------------------------------------------------------------

TEST(PostgresTypeTest, FromSchemaBool) {
  MockTypeResolver resolver;
  ASSERT_EQ(resolver.Init(), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_BOOL);

  PostgresType pg_type;
  ArrowError error;
  ASSERT_EQ(PostgresType::FromSchema(resolver, schema.get(), &pg_type, &error),
            NANOARROW_OK);
  EXPECT_EQ(pg_type.type_id(), PostgresTypeId::kBool);
}

TEST(PostgresTypeTest, FromSchemaDouble) {
  MockTypeResolver resolver;
  ASSERT_EQ(resolver.Init(), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_DOUBLE);

  PostgresType pg_type;
  ArrowError error;
  ASSERT_EQ(PostgresType::FromSchema(resolver, schema.get(), &pg_type, &error),
            NANOARROW_OK);
  EXPECT_EQ(pg_type.type_id(), PostgresTypeId::kFloat8);
}

TEST(PostgresTypeTest, FromSchemaBinary) {
  MockTypeResolver resolver;
  ASSERT_EQ(resolver.Init(), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_BINARY);

  PostgresType pg_type;
  ArrowError error;
  ASSERT_EQ(PostgresType::FromSchema(resolver, schema.get(), &pg_type, &error),
            NANOARROW_OK);
  EXPECT_EQ(pg_type.type_id(), PostgresTypeId::kBytea);
}

TEST(PostgresTypeTest, FromSchemaTimestampNoTz) {
  MockTypeResolver resolver;
  ASSERT_EQ(resolver.Init(), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeDateTime(schema.get(), NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_MICRO, nullptr),
            NANOARROW_OK);

  PostgresType pg_type;
  ArrowError error;
  ASSERT_EQ(PostgresType::FromSchema(resolver, schema.get(), &pg_type, &error),
            NANOARROW_OK);
  EXPECT_EQ(pg_type.type_id(), PostgresTypeId::kTimestamp);
}

TEST(PostgresTypeTest, FromSchemaTimestampWithTz) {
  MockTypeResolver resolver;
  ASSERT_EQ(resolver.Init(), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(ArrowSchemaSetTypeDateTime(schema.get(), NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_MICRO, "UTC"),
            NANOARROW_OK);

  PostgresType pg_type;
  ArrowError error;
  ASSERT_EQ(PostgresType::FromSchema(resolver, schema.get(), &pg_type, &error),
            NANOARROW_OK);
  EXPECT_EQ(pg_type.type_id(), PostgresTypeId::kTimestamptz);
}

TEST(PostgresTypeTest, FromSchemaNullType) {
  MockTypeResolver resolver;
  ASSERT_EQ(resolver.Init(), NANOARROW_OK);

  nanoarrow::UniqueSchema schema;
  ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_NA);

  PostgresType pg_type;
  ArrowError error;
  ASSERT_EQ(PostgresType::FromSchema(resolver, schema.get(), &pg_type, &error),
            NANOARROW_OK);
  // NA defaults to kText
  EXPECT_EQ(pg_type.type_id(), PostgresTypeId::kText);
}

// ---------------------------------------------------------------------------
// PostgresType: accessor methods
// ---------------------------------------------------------------------------

TEST(PostgresTypeTest, FieldNameAccessor) {
  PostgresType t = PostgresType(PostgresTypeId::kInt4).WithFieldName("my_field");
  EXPECT_EQ(t.field_name(), "my_field");
}

TEST(PostgresTypeTest, ChildAccessors) {
  PostgresType record(PostgresTypeId::kRecord);
  record.AppendChild("a", PostgresType(PostgresTypeId::kInt4));
  record.AppendChild("b", PostgresType(PostgresTypeId::kText));
  record.AppendChild("c", PostgresType(PostgresTypeId::kBool));

  EXPECT_EQ(record.n_children(), 3);
  EXPECT_EQ(record.child(0).type_id(), PostgresTypeId::kInt4);
  EXPECT_EQ(record.child(1).type_id(), PostgresTypeId::kText);
  EXPECT_EQ(record.child(2).type_id(), PostgresTypeId::kBool);
  EXPECT_EQ(record.child(0).field_name(), "a");
}

// ---------------------------------------------------------------------------
// PostgresTypeResolver: duplicate OID
// ---------------------------------------------------------------------------

TEST(PostgresTypeTest, ResolverDuplicateOid) {
  PostgresTypeResolver resolver;

  // Insert int4 at OID 23
  PostgresTypeResolver::Item item1 = {23, "int4", "int4recv", 0, 0, 0};
  ArrowError error;
  ASSERT_EQ(resolver.Insert(item1, &error), NANOARROW_OK);

  // Insert text at same OID 23 — std::unordered_map::insert does NOT overwrite
  PostgresTypeResolver::Item item2 = {23, "text", "textrecv", 0, 0, 0};
  ASSERT_EQ(resolver.Insert(item2, &error), NANOARROW_OK);

  PostgresType found;
  ASSERT_EQ(resolver.Find(23, &found, &error), NANOARROW_OK);
  // First insertion wins (no overwrite)
  EXPECT_EQ(found.type_id(), PostgresTypeId::kInt4);
}

}  // namespace adbcpq
