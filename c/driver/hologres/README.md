# ADBC Hologres Driver

A native C/C++ ADBC driver based on libpq for connecting to [Hologres](https://www.alibabacloud.com/product/hologres) (Alibaba Cloud real-time data warehouse). It implements the ADBC API specification, providing high-performance data querying and bulk ingestion capabilities, accessible from Python and Java SDKs.

---

## Table of Contents

- [1. Architecture Overview](#1-architecture-overview)
- [2. Development Guide](#2-development-guide)
- [3. Testing](#3-testing)
- [4. Features](#4-features)
- [5. Configuration Reference](#5-configuration-reference)
- [6. Usage Examples](#6-usage-examples)
- [7. Known Limitations](#7-known-limitations)
- [8. Release Process](#8-release-process)

---

## 1. Architecture Overview

```
┌─────────────────────┐   ┌───────────────────────────┐
│  Python Application │   │  Java Application         │
│  (DBAPI 2.0)        │   │                           │
└────────┬────────────┘   └─────────┬─────────────────┘
         │                          │
         ▼                          ▼
┌─────────────────────┐   ┌───────────────────────────┐
│ adbc_driver_hologres│   │  adbc-driver-jni          │
│  (Python wrapper)   │   │  (Java JNI bridge)        │
│  dlopen(.so)        │   │  dlopen(.so)              │
└────────┬────────────┘   └─────────┬─────────────────┘
         │                          │
         └──────────┬───────────────┘
                    ▼
         ┌─────────────────────┐
         │ libadbc_driver_     │
         │ hologres.so         │
         │ (C/C++ core driver) │
         │ ← libpq             │
         │ ← liblz4            │
         └────────┬────────────┘
                  │
                  ▼
         ┌─────────────────────┐
         │  Hologres Instance  │
         │  (PostgreSQL wire   │
         │   protocol)         │
         └─────────────────────┘
```

### Directory Structure

```
c/driver/hologres/              # C/C++ core driver
├── hologres.cc                 # ADBC entry point, exports AdbcDriverHologresInit
├── database.h/cc               # HologresDatabase: URI parsing, connection pool, type resolver
├── connection.h/cc             # HologresConnection: metadata queries, schema management
├── statement.h/cc              # HologresStatement: SQL execution, COPY ingest, Stage ingest
├── stage_writer.h/cc           # HologresStageWriter: Stage-based bulk ingestion
├── stage_connection.h/cc       # StageConnection: thread-safe connection for stage operations
├── arrow_copy_reader.h/cc      # ArrowCopyReader: Arrow IPC COPY format reader
├── result_helper.h/cc          # PqResultHelper: PGresult lifecycle management
├── result_reader.h/cc          # PqResultArrayReader: result set → Arrow arrays
├── bind_stream.h               # BindStream: parameter binding stream
├── error.h/cc                  # Error handling (SQLSTATE → ADBC error codes)
├── postgres_type.h             # PostgreSQL type system and OID resolution
├── postgres_util.h             # Byte-order conversion utilities
├── copy/                       # COPY protocol reader/writer
│   ├── copy_common.h           # COPY binary signature constants
│   ├── reader.h                # PostgresCopyStreamReader
│   └── writer.h                # PostgresCopyStreamWriter
├── test_util.h                 # Test helpers (MockTypeResolver, etc.)
└── *_test.cc                   # 12 unit test files

c/include/arrow-adbc/driver/
└── hologres.h                  # Public C API header

python/adbc_driver_hologres/    # Python wrapper layer
├── adbc_driver_hologres/
│   ├── __init__.py             # Enum definitions, connect(), driver path resolution
│   └── dbapi.py                # DBAPI 2.0 interface
├── tests/                      # Integration tests (~60 test cases)
├── benchmarks/                 # ASV performance benchmarks
├── pyproject.toml              # Package metadata
└── setup.py                    # Build script

java/driver/jni/                # Java JNI bridge (generic, not Hologres-specific)
├── src/main/java/.../jni/
│   ├── JniDriver.java          # Driver factory
│   ├── JniDatabase.java        # Database handle
│   ├── JniConnection.java      # Connection handle
│   └── JniStatement.java       # Statement handle
└── src/main/cpp/
    └── jni_wrapper.cc          # C++ JNI implementation
```

---

## 2. Development Guide

### 2.1 C/C++ Core Driver

#### Dependencies

| Dependency | Description |
|------------|-------------|
| **libpq** | PostgreSQL client library (found via pkg-config) |
| **liblz4** | LZ4 compression library (for Arrow LZ4 COPY format) |
| **nanoarrow** | Arrow C Data Interface implementation (bundled) |
| **fmt** | String formatting library |

#### Building

```bash
# From the project root
mkdir build && cd build
cmake ../c \
  -DADBC_DRIVER_HOLOGRES=ON \
  -DADBC_BUILD_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Debug
cmake --build . --parallel
```

Or using presets (if available):

```bash
cmake -S c -B build --preset debug -DADBC_DRIVER_HOLOGRES=ON
cmake --build build --parallel
```

Build output: `libadbc_driver_hologres.so` (Linux), `.dylib` (macOS), or `.dll` (Windows).

#### macOS Notes

On macOS, install libpq and lz4 via Homebrew:

```bash
brew install libpq lz4
export PKG_CONFIG_PATH="$(brew --prefix libpq)/lib/pkgconfig:$(brew --prefix lz4)/lib/pkgconfig:$PKG_CONFIG_PATH"
```

### 2.2 Python SDK

#### Prerequisites

A pre-built C driver shared library is required.

#### Installation

```bash
# Set the C driver library path
export ADBC_HOLOGRES_LIBRARY=/path/to/libadbc_driver_hologres.so

# Install (with test dependencies)
pip install -e "python/adbc_driver_hologres[test]"
```

Alternatively, build everything via CMake:

```bash
cmake -S c -B build \
  -DADBC_DRIVER_HOLOGRES=ON \
  -DADBC_BUILD_PYTHON=ON
cmake --build build --target python
```

#### Dependencies

- `adbc-driver-manager` -- ADBC driver manager
- `importlib-resources >= 1.3`
- Optional: `pandas`, `pyarrow >= 14.0.1` (for DBAPI `fetch_df()` etc.)

### 2.3 Java SDK (JNI Bridge)

Java accesses the C driver through the **generic JNI bridge** `adbc-driver-jni`. There is no Hologres-specific Java module.

#### Building

```bash
# 1. Build the C driver and JNI native library
cmake -S c -B build \
  -DADBC_DRIVER_HOLOGRES=ON \
  -DADBC_DRIVER_JNI=ON
cmake --build build --parallel

# 2. Build the Java JAR (from the java/ directory)
cd java
mvn install -Pjni
```

#### Runtime Dependencies

- `libadbc_driver_hologres.so` -- C driver shared library
- `libadbc_driver_jni.so` -- JNI bridge native library
- `adbc-driver-jni-<version>.jar` -- Java JAR

---

## 3. Testing

### 3.1 C Unit Tests

12 unit test files, **no live database required** (uses MockTypeResolver and MockStageConnection).

| Test File | Coverage |
|-----------|----------|
| `database_test.cc` | URI parsing, FixedFE URI generation, version detection |
| `connection_test.cc` | Connection lifecycle, GetInfo, schema operations |
| `statement_test.cc` | Statement option parsing, SQL queries, bulk ingest modes |
| `stage_writer_test.cc` | Stage writer: create/write/insert/drop stage, parallel uploads |
| `arrow_copy_reader_test.cc` | Arrow IPC COPY reading, LZ4 decompression |
| `copy_test.cc` | PG binary COPY reader (all data types) |
| `copy_writer_test.cc` | PG binary COPY writer (all data types) |
| `bind_stream_test.cc` | Bind parameter stream handling |
| `result_helper_test.cc` | PqResultHelper parsing |
| `postgres_type_test.cc` | PostgreSQL type system and OID resolution |
| `postgres_util_test.cc` | Byte-order conversion utilities |
| `error_test.cc` | Error handling and SQLSTATE mapping |

Running tests:

```bash
cd build
ctest -L driver-hologres              # Run all Hologres tests
ctest -R adbc-driver-hologres-stage   # Run a specific test
ctest -L driver-hologres -V           # Verbose output
```

### 3.2 Python Integration Tests

~60 integration tests, **requires a live Hologres instance**.

Test coverage:
- Connection & metadata: `current_catalog`, `current_db_schema`, `get_info`, `get_table_types`, `get_objects`
- Basic queries: `SELECT`, DDL, `execute_schema`, `fetch_arrow`
- Bulk ingest (COPY mode): `create`, `append`, `replace`, `create_append`
- Bulk ingest (Stage mode): basic ingest, conflict handling
- ON_CONFLICT: `ignore`, `update`, `none` (error)
- Data types: temporal types, Decimal, JSON/JSONB, List, Dictionary, etc.
- COPY format variants: read tests with `binary`, `arrow`, `arrow_lz4`
- Statistics: `get_statistics`, `get_statistic_names`

Running tests:

```bash
export ADBC_HOLOGRES_TEST_URI="postgresql://user:pass@host:port/dbname"
cd python/adbc_driver_hologres
pytest tests/ -vvx
pytest tests/test_dbapi.py::test_ingest_stage_mode -vvx  # Run a single test
```

### 3.3 Python Benchmarks

Uses the [Airspeed Velocity (ASV)](https://asv.readthedocs.io/) framework, comparing ADBC against asyncpg / psycopg2 / DuckDB.

| Suite | Description |
|-------|-------------|
| `HologresOneColumnSuite` | Single-column reads, parameterized by row count (10K–10M) and data type |
| `HologresMultiColumnSuite` | Multi-column reads (4 columns), same parameter matrix |
| `HologresIngestSuite` | Bulk ingest comparison: ADBC COPY/Stage vs asyncpg vs psycopg2 |
| `HologresVectorIngestSuite` | Vector data ingest (512/1024-dim FLOAT4[]), large-batch support |

Running benchmarks:

```bash
export ADBC_HOLOGRES_TEST_URI="postgresql://user:pass@host:port/dbname"
cd python/adbc_driver_hologres
asv run
```

### 3.4 Java Tests

The JNI bridge has generic tests (using the SQLite backend). There are no Hologres-specific Java tests at this time.

---

## 4. Features

### Feature Matrix

| Feature | Supported | Notes |
|---------|:---------:|-------|
| SQL query execution | ✅ | `SetSqlQuery()` + `ExecuteQuery()` |
| Prepared statements | ✅ | `Prepare()` + parameter binding + execute |
| Parameter binding | ✅ | Via Arrow C Data Interface |
| Output schema retrieval | ✅ | `ExecuteSchema()` returns schema without executing |
| Query cancellation | ✅ | `Cancel()` via `PQcancel` |
| Bulk ingest -- COPY | ✅ | Default mode, based on PG COPY protocol |
| Bulk ingest -- Stage | ✅ | Hologres >= 4.1, Arrow IPC + internal temporary storage |
| ON_CONFLICT handling | ✅ | none / ignore / update |
| Ingest modes | ✅ | create / append / replace / create_append |
| COPY read formats | ✅ | binary / arrow / arrow_lz4 |
| GetInfo | ✅ | Vendor name, driver version, etc. |
| GetObjects | ✅ | catalog / schema / table / column / constraint |
| GetTableSchema | ✅ | Column names and types |
| GetTableTypes | ✅ | table, view, materialized_view, foreign_table, etc. |
| GetStatistics | ✅ | Approximate stats: row count, null count, avg byte width, distinct count |
| Transactions | ❌ | Hologres does not support transactions; autocommit is always on |
| ReadPartition | ❌ | Not supported |
| ExecutePartitions | ❌ | Not supported |
| Substrait | ❌ | Not supported |

### Bulk Ingest Modes

#### COPY Mode (Default)

Based on the PostgreSQL `COPY ... FROM STDIN` protocol with the Hologres-specific `STREAM_MODE TRUE` extension:

```
COPY <table> FROM STDIN WITH (FORMAT binary, STREAM_MODE TRUE)
```

- Works with all Hologres versions
- Data serialized via PG binary COPY protocol
- Supports ON_CONFLICT (ignore / update)
- Builds the entire data stream in memory; very large datasets may cause OOM

#### Stage Mode

Based on Hologres internal stage storage (requires Hologres >= 4.1):

```
CreateStage() → WriteToStage() → InsertFromStage() → DropStage()
```

- Serializes Arrow data as IPC format, uploads `.arrow` files to internal stage
- Uses multi-threaded parallel uploads (default: 4 threads)
- Executes COPY IN through a FixedFE connection
- Completes ingestion via `INSERT ... SELECT FROM EXTERNAL_FILES(...)`
- Suitable for large-scale ingestion (10M+ rows)
- Supports type casting for json/jsonb/roaringbitmap columns

### COPY Read Formats

| Format | Enum | Description |
|--------|------|-------------|
| `binary` | `CopyFormat::kBinary` | Standard PostgreSQL binary COPY |
| `arrow` | `CopyFormat::kArrow` | Arrow IPC wrapped in PG COPY framing (Hologres extension) |
| `arrow_lz4` | `CopyFormat::kArrowLz4` | LZ4-compressed Arrow IPC (Hologres extension, **default**) |

Arrow / Arrow LZ4 formats generally offer faster read performance than binary. When query results contain JSONB columns, the driver automatically wraps JSONB to TEXT for Arrow COPY format compatibility.

---

## 5. Configuration Reference

### 5.1 Database Level

| Option | Type | Required | Description |
|--------|------|:--------:|-------------|
| `uri` (`ADBC_OPTION_URI`) | string | ✅ | PostgreSQL connection URI. The driver auto-appends `application_name=adbc_hologres_<version>` |

URI example:
```
postgresql://user:password@host:port/database
```

### 5.2 Connection Level

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `adbc.connection.autocommit` | string | `"true"` | Always true. Setting to `"false"` returns `NOT_IMPLEMENTED` |
| `adbc.connection.current_db_schema` | string | -- | Set: executes `SET search_path TO <value>`. Get: executes `SELECT CURRENT_SCHEMA()` |
| `adbc.connection.current_catalog` | string | -- | Read-only, returns `PQdb(conn_)` |

### 5.3 Statement Level (Hologres-Specific)

| Option | Macro | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `adbc.hologres.batch_size_hint_bytes` | `ADBC_HOLOGRES_OPTION_BATCH_SIZE_HINT_BYTES` | int64 | 16777216 (16 MiB) | Target batch size in bytes for returned results |
| `adbc.hologres.use_copy` | `ADBC_HOLOGRES_OPTION_USE_COPY` | bool | `true` | Whether to use COPY protocol optimization for query reads |
| `adbc.hologres.copy_format` | `ADBC_HOLOGRES_OPTION_COPY_FORMAT` | string | `"arrow_lz4"` | COPY TO STDOUT format: `binary` / `arrow` / `arrow_lz4` |
| `adbc.hologres.on_conflict` | `ADBC_HOLOGRES_OPTION_ON_CONFLICT` | string | `"none"` | Conflict handling for bulk ingest: `none` (error) / `ignore` (skip) / `update` (upsert) |
| `adbc.hologres.ingest_mode` | `ADBC_HOLOGRES_OPTION_INGEST_MODE` | string | `"copy"` | Bulk ingest method: `copy` (PG COPY) / `stage` (Hologres Stage) |

### 5.4 Standard ADBC Ingest Options

| Option | Description |
|--------|-------------|
| `adbc.ingest.target_table` | Target table name |
| `adbc.ingest.target_db_schema` | Target schema |
| `adbc.ingest.mode` | `create` / `append` / `replace` / `create_append` |
| `adbc.ingest.temporary` | `true` / `false` (use temporary table) |

### 5.5 Stage Writer Internal Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ttl_seconds` | 7200 | Stage file time-to-live (seconds) |
| `batch_size` | 8192 | Rows per batch (Arrow IPC serialization) |
| `target_file_size` | 10 MiB | Target size per `.arrow` file |
| `upload_concurrency` | 4 | Number of parallel upload threads |

---

## 6. Usage Examples

### 6.1 Python -- DBAPI 2.0 (Recommended)

#### Basic Query

```python
import adbc_driver_hologres.dbapi as dbapi

uri = "postgresql://user:pass@host:port/dbname"

with dbapi.connect(uri, autocommit=True) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM my_table LIMIT 10")

        # Get a PyArrow Table
        table = cur.fetch_arrow_table()

        # Or get a Pandas DataFrame
        df = cur.fetch_df()
```

#### Bulk Ingest -- COPY Mode

```python
import pyarrow

data = pyarrow.table({
    "id": [1, 2, 3],
    "name": ["a", "b", "c"],
    "value": [1.0, 2.0, 3.0],
})

with dbapi.connect(uri, autocommit=True) as conn:
    with conn.cursor() as cur:
        cur.adbc_ingest("my_table", data, mode="append")
```

#### Bulk Ingest -- Stage Mode

```python
from adbc_driver_hologres import StatementOptions, HologresIngestMode

with dbapi.connect(uri, autocommit=True) as conn:
    with conn.cursor() as cur:
        # Use Stage mode
        cur.adbc_statement.set_options(**{
            StatementOptions.INGEST_MODE.value: HologresIngestMode.STAGE.value,
        })
        cur.adbc_ingest("my_table", data, mode="append")
```

#### ON_CONFLICT Handling

```python
from adbc_driver_hologres import StatementOptions, HologresOnConflict

with dbapi.connect(uri, autocommit=True) as conn:
    with conn.cursor() as cur:
        # Upsert on conflict
        cur.adbc_statement.set_options(**{
            StatementOptions.ON_CONFLICT.value: HologresOnConflict.UPDATE.value,
        })
        cur.adbc_ingest("my_table", data, mode="append")
```

#### Setting COPY Read Format

```python
from adbc_driver_hologres import StatementOptions

with dbapi.connect(uri, autocommit=True) as conn:
    with conn.cursor() as cur:
        # Use standard PG binary format (compatible with JSONB columns)
        cur.adbc_statement.set_options(**{
            StatementOptions.COPY_FORMAT.value: "binary",
        })
        cur.execute("SELECT * FROM table_with_jsonb")
        table = cur.fetch_arrow_table()
```

### 6.2 Python -- Low-Level API

```python
import adbc_driver_hologres
import adbc_driver_manager
import pyarrow

# Create database connection
db = adbc_driver_hologres.connect("postgresql://user:pass@host:port/dbname")
conn = adbc_driver_manager.AdbcConnection(db)

# Execute query
stmt = adbc_driver_manager.AdbcStatement(conn)
stmt.set_sql_query("SELECT 1 AS num")
handle, rows_affected = stmt.execute_query()

# Import Arrow data
reader = pyarrow.RecordBatchReader._import_from_c(handle.address)
table = reader.read_all()
print(table)  # pyarrow.Table: num: int32

# Cleanup
stmt.close()
conn.close()
db.close()
```

### 6.3 Java -- JNI Bridge

```java
import org.apache.arrow.adbc.driver.jni.JniDriver;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

BufferAllocator allocator = new RootAllocator();
JniDriver driver = new JniDriver(allocator);

// Configure connection parameters
Map<String, Object> params = new HashMap<>();
JniDriver.PARAM_DRIVER.set(params, "adbc_driver_hologres");  // or full path to .so
AdbcDriver.PARAM_URI.set(params, "postgresql://user:pass@host:port/dbname");

// Open database and connection
try (AdbcDatabase db = driver.open(params);
     AdbcConnection conn = db.connect();
     AdbcStatement stmt = conn.createStatement()) {

    // Execute query
    stmt.setSqlQuery("SELECT * FROM my_table LIMIT 10");
    AdbcStatement.QueryResult result = stmt.executeQuery();
    ArrowReader reader = result.getReader();

    while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        System.out.println(root.contentToTSVString());
    }
}
```

### 6.4 C/C++ -- Native API

```c
#include <arrow-adbc/adbc.h>
#include <arrow-adbc/driver/hologres.h>

struct AdbcError error = ADBC_ERROR_INIT;

// Create database
struct AdbcDatabase database;
AdbcDatabaseNew(&database, &error);
AdbcDatabaseSetOption(&database, "driver", "adbc_driver_hologres", &error);
AdbcDatabaseSetOption(&database, "uri",
                      "postgresql://user:pass@host:port/dbname", &error);
AdbcDatabaseInit(&database, &error);

// Create connection
struct AdbcConnection connection;
AdbcConnectionNew(&connection, &error);
AdbcConnectionInit(&connection, &database, &error);

// Execute query
struct AdbcStatement statement;
AdbcStatementNew(&connection, &statement, &error);
AdbcStatementSetSqlQuery(&statement, "SELECT * FROM my_table", &error);

struct ArrowArrayStream stream;
int64_t rows_affected;
AdbcStatementExecuteQuery(&statement, &stream, &rows_affected, &error);

// Process results...

// Cleanup
AdbcStatementRelease(&statement, &error);
AdbcConnectionRelease(&connection, &error);
AdbcDatabaseRelease(&database, &error);
```

---

## 7. Known Limitations

| Limitation | Description |
|------------|-------------|
| **No transaction support** | Hologres does not support transactions. `Commit()` / `Rollback()` return `NOT_IMPLEMENTED`; autocommit is always on |
| **Stage mode version requirement** | Stage mode requires Hologres >= 4.1 |
| **JSONB + Arrow COPY** | JSONB columns cannot be read directly via `arrow` / `arrow_lz4` format. The driver auto-wraps JSONB to TEXT, but for direct JSONB queries use `binary` format or explicit `CAST(col AS TEXT)` in SQL |
| **INTERVAL type** | The COPY binary writer does not support `month_day_nano` interval type |
| **COPY mode memory** | COPY mode builds the entire data stream in memory; very large datasets (10M+ rows with large fields) may cause OOM. Use Stage mode for large-scale ingestion |
| **Stage mode NULL handling** | In Stage mode, NULL values are converted to type-specific defaults (int→0, float→0.0, string→"", bool→false) |
| **Parameterized queries** | `$1`-style parameterized queries may not be supported on some Hologres versions |
| **Java tests** | The JNI bridge currently has no Hologres-specific integration tests |

---

## 8. Release Process

### C Driver

The C driver is built as part of the ADBC project CMake build:

```bash
cmake -S c -B build -DADBC_DRIVER_HOLOGRES=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

Output: `libadbc_driver_hologres.{so,dylib,dll}`

### Python Package

1. Build the C driver shared library
2. Set the `ADBC_HOLOGRES_LIBRARY` environment variable to point to the build output
3. Build the Python package:

```bash
cd python/adbc_driver_hologres
python -m build
```

Output: `adbc-driver-hologres-<version>.whl` (with embedded shared library)

### Java

The Java JNI bridge is released as a generic component; no Hologres-specific JAR is needed. At runtime, ensure `libadbc_driver_hologres.so` is on `LD_LIBRARY_PATH` or specify the full path via `JniDriver.PARAM_DRIVER`.

```bash
cd java
mvn install -Pjni
```

Output: `adbc-driver-jni-<version>.jar`
