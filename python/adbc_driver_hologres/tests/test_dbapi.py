# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Integration tests for the Hologres ADBC DBAPI driver.

These tests require a live Hologres instance.  Set the environment variable
``ADBC_HOLOGRES_TEST_URI`` to a valid connection URI before running.

Key differences from the PostgreSQL driver tests:
- Hologres does not support transactions (autocommit only).
- GENERATE_SERIES is not available; data is prepared via ADBC ingest.
- Parameterized queries with ``$1`` syntax may not be supported.
- Stage mode and ON_CONFLICT options are Hologres-specific features.
"""

import datetime
import decimal
from typing import Generator

import pyarrow
import pytest

import adbc_driver_hologres
from adbc_driver_hologres import StatementOptions, dbapi


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def hologres(hologres_uri: str) -> Generator[dbapi.Connection, None, None]:
    with dbapi.connect(hologres_uri, autocommit=True) as conn:
        yield conn


# ---------------------------------------------------------------------------
# Connection & metadata
# ---------------------------------------------------------------------------


def test_conn_current_catalog(hologres: dbapi.Connection) -> None:
    assert hologres.adbc_current_catalog != ""


def test_conn_current_db_schema(hologres: dbapi.Connection) -> None:
    assert hologres.adbc_current_db_schema == "public"


def test_conn_change_db_schema(hologres: dbapi.Connection) -> None:
    assert hologres.adbc_current_db_schema == "public"

    # Note: Hologres reserves the "hg_" prefix for system schemas
    with hologres.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS adbc_dbapi_schema")

    hologres.adbc_current_db_schema = "adbc_dbapi_schema"
    assert hologres.adbc_current_db_schema == "adbc_dbapi_schema"

    # Restore
    hologres.adbc_current_db_schema = "public"


def test_conn_get_info(hologres: dbapi.Connection) -> None:
    info = hologres.adbc_get_info()
    assert info["vendor_name"] == "Hologres"
    assert info["driver_name"] == "ADBC Hologres Driver"
    assert info["driver_adbc_version"] == 1_001_000


def test_conn_get_table_types(hologres: dbapi.Connection) -> None:
    result = hologres.adbc_get_table_types()
    # The DBAPI may return a list or a RecordBatchReader depending on version
    if hasattr(result, "read_all"):
        table = result.read_all()
        types = table.column("table_type").to_pylist()
    else:
        # Result is already a list of table type strings
        types = result
    assert "table" in types


def test_conn_get_table_schema(hologres: dbapi.Connection) -> None:
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_schema_tbl")
        cur.execute(
            "CREATE TABLE hg_test_schema_tbl "
            "(id INT NOT NULL, name TEXT, value DOUBLE PRECISION)"
        )

    schema = hologres.adbc_get_table_schema("hg_test_schema_tbl")
    assert "id" in schema.names
    assert "name" in schema.names
    assert "value" in schema.names

    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_schema_tbl")


def test_conn_get_objects(hologres: dbapi.Connection) -> None:
    table_name = "hg_test_get_objects"

    with hologres.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(f"CREATE TABLE {table_name} (id INT)")

    metadata = (
        hologres.adbc_get_objects(
            depth="tables",
            db_schema_filter="public",
            table_name_filter=table_name,
        )
        .read_all()
        .to_pylist()
    )

    catalog_name = hologres.adbc_current_catalog
    catalog = next(
        (row for row in metadata if row["catalog_name"] == catalog_name), None
    )
    assert catalog is not None

    schemas = catalog["catalog_db_schemas"]
    assert len(schemas) >= 1
    public_schema = next(
        (s for s in schemas if s["db_schema_name"] == "public"), None
    )
    assert public_schema is not None
    tables = public_schema["db_schema_tables"]
    found = any(t["table_name"] == table_name for t in tables)
    assert found, f"Table {table_name} not found in get_objects result"

    with hologres.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_conn_autocommit_only(hologres_uri: str) -> None:
    """Hologres does not support transactions; disabling autocommit should fail."""
    conn = dbapi.connect(hologres_uri, autocommit=True)
    try:
        with pytest.raises(dbapi.NotSupportedError):
            conn.adbc_connection.set_options(
                **{"adbc.connection.autocommit": "false"}
            )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Basic queries
# ---------------------------------------------------------------------------


def test_query_trivial(hologres: dbapi.Connection) -> None:
    with hologres.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)


def test_query_execute_schema(hologres: dbapi.Connection) -> None:
    with hologres.cursor() as cur:
        schema = cur.adbc_execute_schema("SELECT 1 AS foo")
        assert schema == pyarrow.schema([("foo", "int32")])


def test_query_invalid(hologres: dbapi.Connection) -> None:
    with hologres.cursor() as cur:
        with pytest.raises(hologres.ProgrammingError):
            cur.execute("SELECT * FROM table_does_not_exist_xyz")


def test_ddl(hologres: dbapi.Connection) -> None:
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_ddl")
        cur.execute("CREATE TABLE hg_test_ddl (ints INT)")
        cur.execute("INSERT INTO hg_test_ddl VALUES (1)")
        cur.execute("SELECT * FROM hg_test_ddl")
        assert cur.fetchone() == (1,)
        cur.execute("DROP TABLE IF EXISTS hg_test_ddl")


def test_query_fetch_arrow(hologres: dbapi.Connection) -> None:
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_arrow")
        cur.execute("CREATE TABLE hg_test_arrow (id INT, name TEXT)")
        cur.execute("INSERT INTO hg_test_arrow VALUES (1, 'alice')")
        cur.execute("INSERT INTO hg_test_arrow VALUES (2, 'bob')")

        cur.execute("SELECT id, name FROM hg_test_arrow ORDER BY id")
        table = cur.fetch_arrow_table()
        assert table.num_rows == 2
        assert table.column("id").to_pylist() == [1, 2]
        assert table.column("name").to_pylist() == ["alice", "bob"]

        cur.execute("DROP TABLE IF EXISTS hg_test_arrow")


# ---------------------------------------------------------------------------
# Bulk ingest — COPY mode
# ---------------------------------------------------------------------------


def _make_test_table() -> pyarrow.Table:
    return pyarrow.table(
        {
            "ints": pyarrow.array([1, 2, 3], type=pyarrow.int32()),
            "strs": pyarrow.array(["a", None, "b"], type=pyarrow.string()),
        }
    )


def test_ingest_create(hologres: dbapi.Connection) -> None:
    table = _make_test_table()
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_ingest_create")
        cur.adbc_ingest("hg_test_ingest_create", table, mode="create")

        cur.execute("SELECT ints, strs FROM hg_test_ingest_create ORDER BY ints")
        result = cur.fetch_arrow_table()
        assert result == table

        cur.execute("DROP TABLE IF EXISTS hg_test_ingest_create")


def test_ingest_append(hologres: dbapi.Connection) -> None:
    table = _make_test_table()
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_ingest_append")
        cur.adbc_ingest("hg_test_ingest_append", table, mode="create")
        cur.adbc_ingest("hg_test_ingest_append", table, mode="append")

        cur.execute(
            "SELECT count(*) FROM hg_test_ingest_append"
        )
        assert cur.fetchone() == (6,)

        cur.execute("DROP TABLE IF EXISTS hg_test_ingest_append")


def test_ingest_replace(hologres: dbapi.Connection) -> None:
    table = _make_test_table()
    replacement = pyarrow.table(
        {
            "ints": pyarrow.array([10, 20], type=pyarrow.int32()),
            "strs": pyarrow.array(["x", "y"], type=pyarrow.string()),
        }
    )
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_ingest_replace")
        cur.adbc_ingest("hg_test_ingest_replace", table, mode="create")
        cur.adbc_ingest("hg_test_ingest_replace", replacement, mode="replace")

        cur.execute(
            "SELECT ints, strs FROM hg_test_ingest_replace ORDER BY ints"
        )
        result = cur.fetch_arrow_table()
        assert result == replacement

        cur.execute("DROP TABLE IF EXISTS hg_test_ingest_replace")


def test_ingest_create_append(hologres: dbapi.Connection) -> None:
    table = _make_test_table()
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_ingest_ca")
        cur.adbc_ingest("hg_test_ingest_ca", table, mode="create_append")

        cur.execute("SELECT ints, strs FROM hg_test_ingest_ca ORDER BY ints")
        result = cur.fetch_arrow_table()
        assert result == table

        # Append again via create_append (table already exists)
        cur.adbc_ingest("hg_test_ingest_ca", table, mode="create_append")
        cur.execute("SELECT count(*) FROM hg_test_ingest_ca")
        assert cur.fetchone() == (6,)

        cur.execute("DROP TABLE IF EXISTS hg_test_ingest_ca")


def test_ingest_schema(hologres: dbapi.Connection) -> None:
    table = pyarrow.table(
        {"numbers": pyarrow.array([1, 2], type=pyarrow.int32())}
    )
    # Note: Hologres reserves the "hg_" prefix for system schemas
    with hologres.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS adbc_test_schema")
        cur.execute("DROP TABLE IF EXISTS adbc_test_schema.ingest_tbl")

        cur.adbc_ingest(
            "ingest_tbl", table, mode="create", db_schema_name="adbc_test_schema"
        )

        cur.execute(
            "SELECT numbers FROM adbc_test_schema.ingest_tbl ORDER BY numbers"
        )
        assert cur.fetch_arrow_table() == table

        cur.execute("DROP TABLE IF EXISTS adbc_test_schema.ingest_tbl")


def test_ingest_types(hologres: dbapi.Connection) -> None:
    """Round-trip test for multiple Arrow data types."""
    table = pyarrow.table(
        {
            "col_bool": pyarrow.array([True, False, None], type=pyarrow.bool_()),
            "col_int16": pyarrow.array([1, -1, None], type=pyarrow.int16()),
            "col_int32": pyarrow.array([100, -100, None], type=pyarrow.int32()),
            "col_int64": pyarrow.array([1000, -1000, None], type=pyarrow.int64()),
            "col_float32": pyarrow.array(
                [1.5, -2.5, None], type=pyarrow.float32()
            ),
            "col_float64": pyarrow.array(
                [3.14, -2.71, None], type=pyarrow.float64()
            ),
            "col_string": pyarrow.array(["hello", "world", None]),
            "col_binary": pyarrow.array([b"\x01\x02", b"\x03", None]),
        }
    )
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_types")
        cur.adbc_ingest("hg_test_types", table, mode="create")

        cur.execute(
            "SELECT col_bool, col_int16, col_int32, col_int64, "
            "col_float32, col_float64, col_string, col_binary "
            "FROM hg_test_types ORDER BY col_int32"
        )
        result = cur.fetch_arrow_table()
        assert result.num_rows == 3

        # Verify non-null values
        assert result.column("col_int32").to_pylist()[0] == -100
        assert result.column("col_string").to_pylist()[1] == "hello"

        cur.execute("DROP TABLE IF EXISTS hg_test_types")


# ---------------------------------------------------------------------------
# Hologres-specific: ON_CONFLICT
# ---------------------------------------------------------------------------


def _make_pk_table(conn: dbapi.Connection, table_name: str) -> None:
    """Create a table with a primary key for ON_CONFLICT tests."""
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(
            f"CREATE TABLE {table_name} "
            f"(id INT PRIMARY KEY, value TEXT)"
        )


def test_ingest_on_conflict_ignore(hologres: dbapi.Connection) -> None:
    table_name = "hg_test_conflict_ignore"
    _make_pk_table(hologres, table_name)

    initial = pyarrow.table(
        {
            "id": pyarrow.array([1, 2, 3], type=pyarrow.int32()),
            "value": pyarrow.array(["a", "b", "c"]),
        }
    )
    conflict = pyarrow.table(
        {
            "id": pyarrow.array([2, 3, 4], type=pyarrow.int32()),
            "value": pyarrow.array(["B_new", "C_new", "d"]),
        }
    )

    with hologres.cursor() as cur:
        cur.adbc_ingest(table_name, initial, mode="append")

        # Ingest with ON_CONFLICT ignore: conflicting rows should be skipped
        cur.adbc_statement.set_options(
            **{StatementOptions.ON_CONFLICT.value: "ignore"}
        )
        cur.adbc_ingest(table_name, conflict, mode="append")

        cur.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
        result = cur.fetch_arrow_table()
        values = dict(
            zip(
                result.column("id").to_pylist(),
                result.column("value").to_pylist(),
            )
        )
        assert values[2] == "b"  # original, not "B_new"
        assert values[3] == "c"  # original, not "C_new"
        assert values[4] == "d"  # new row inserted

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_ingest_on_conflict_update(hologres: dbapi.Connection) -> None:
    table_name = "hg_test_conflict_update"
    _make_pk_table(hologres, table_name)

    initial = pyarrow.table(
        {
            "id": pyarrow.array([1, 2, 3], type=pyarrow.int32()),
            "value": pyarrow.array(["a", "b", "c"]),
        }
    )
    conflict = pyarrow.table(
        {
            "id": pyarrow.array([2, 3, 4], type=pyarrow.int32()),
            "value": pyarrow.array(["B_new", "C_new", "d"]),
        }
    )

    with hologres.cursor() as cur:
        cur.adbc_ingest(table_name, initial, mode="append")

        # Ingest with ON_CONFLICT update: conflicting rows should be updated
        cur.adbc_statement.set_options(
            **{StatementOptions.ON_CONFLICT.value: "update"}
        )
        cur.adbc_ingest(table_name, conflict, mode="append")

        cur.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
        result = cur.fetch_arrow_table()
        values = dict(
            zip(
                result.column("id").to_pylist(),
                result.column("value").to_pylist(),
            )
        )
        assert values[2] == "B_new"  # updated
        assert values[3] == "C_new"  # updated
        assert values[4] == "d"  # new row

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


# ---------------------------------------------------------------------------
# Hologres-specific: Stage mode
# ---------------------------------------------------------------------------


def test_ingest_stage_mode(hologres: dbapi.Connection) -> None:
    table_name = "adbc_test_stage"
    # Stage mode may convert NULLs to empty strings, so use non-null data
    table = pyarrow.table(
        {
            "ints": pyarrow.array([1, 2, 3], type=pyarrow.int32()),
            "strs": pyarrow.array(["a", "b", "c"], type=pyarrow.string()),
        }
    )

    with hologres.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(
            f"CREATE TABLE {table_name} (ints INT, strs TEXT)"
        )

        cur.adbc_statement.set_options(
            **{StatementOptions.INGEST_MODE.value: "stage"}
        )
        cur.adbc_ingest(table_name, table, mode="append")

        cur.execute(f"SELECT ints, strs FROM {table_name} ORDER BY ints")
        result = cur.fetch_arrow_table()
        assert result.num_rows == 3
        assert result.column("ints").to_pylist() == [1, 2, 3]
        assert result.column("strs").to_pylist() == ["a", "b", "c"]

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_ingest_stage_on_conflict_ignore(hologres: dbapi.Connection) -> None:
    table_name = "hg_test_stage_ignore"
    _make_pk_table(hologres, table_name)

    initial = pyarrow.table(
        {
            "id": pyarrow.array([1, 2], type=pyarrow.int32()),
            "value": pyarrow.array(["a", "b"]),
        }
    )
    conflict = pyarrow.table(
        {
            "id": pyarrow.array([2, 3], type=pyarrow.int32()),
            "value": pyarrow.array(["B_new", "c"]),
        }
    )

    with hologres.cursor() as cur:
        cur.adbc_ingest(table_name, initial, mode="append")

        cur.adbc_statement.set_options(
            **{
                StatementOptions.INGEST_MODE.value: "stage",
                StatementOptions.ON_CONFLICT.value: "ignore",
            }
        )
        cur.adbc_ingest(table_name, conflict, mode="append")

        cur.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
        result = cur.fetch_arrow_table()
        values = dict(
            zip(
                result.column("id").to_pylist(),
                result.column("value").to_pylist(),
            )
        )
        assert values[2] == "b"  # original kept
        assert values[3] == "c"  # new row

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_ingest_stage_on_conflict_update(hologres: dbapi.Connection) -> None:
    table_name = "adbc_test_stage_update"
    _make_pk_table(hologres, table_name)

    initial = pyarrow.table(
        {
            "id": pyarrow.array([1, 2], type=pyarrow.int32()),
            "value": pyarrow.array(["a", "b"]),
        }
    )
    conflict = pyarrow.table(
        {
            "id": pyarrow.array([2, 3], type=pyarrow.int32()),
            "value": pyarrow.array(["B_new", "c"]),
        }
    )

    with hologres.cursor() as cur:
        cur.adbc_ingest(table_name, initial, mode="append")

        cur.adbc_statement.set_options(
            **{
                StatementOptions.INGEST_MODE.value: "stage",
                StatementOptions.ON_CONFLICT.value: "update",
            }
        )
        cur.adbc_ingest(table_name, conflict, mode="append")

        cur.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
        result = cur.fetch_arrow_table()
        values = dict(
            zip(
                result.column("id").to_pylist(),
                result.column("value").to_pylist(),
            )
        )
        assert values[2] == "B_new"  # updated
        assert values[3] == "c"  # new row

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


# ---------------------------------------------------------------------------
# Hologres-specific: statement options
# ---------------------------------------------------------------------------


def test_batch_size_hint(hologres: dbapi.Connection) -> None:
    """Setting batch_size_hint_bytes should affect the number of batches."""
    import numpy as np

    table_name = "hg_test_batch_size"
    # Prepare data: 10000 ints
    data = pyarrow.table(
        {"ints": pyarrow.array(np.arange(10000, dtype=np.int32()))}
    )

    with hologres.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.adbc_ingest(table_name, data, mode="create")

        # Default batch size (16 MB) — should yield 1 batch for 40 KB data
        cur.execute(f"SELECT * FROM {table_name}")
        result = cur.fetch_arrow_table()
        default_batches = len(result.to_batches())

        # Tiny batch size — should yield many batches
        cur.adbc_statement.set_options(
            **{StatementOptions.BATCH_SIZE_HINT_BYTES.value: "1"}
        )
        cur.execute(f"SELECT * FROM {table_name}")
        result = cur.fetch_arrow_table()
        tiny_batches = len(result.to_batches())

        assert tiny_batches > default_batches

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_use_copy_disabled(hologres: dbapi.Connection) -> None:
    """Queries should still work when use_copy is disabled."""
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_no_copy")
        cur.execute("CREATE TABLE hg_test_no_copy (id INT, name TEXT)")
        cur.execute("INSERT INTO hg_test_no_copy VALUES (1, 'test')")

        cur.adbc_statement.set_options(
            **{StatementOptions.USE_COPY.value: "false"}
        )
        cur.execute("SELECT id, name FROM hg_test_no_copy")
        result = cur.fetch_arrow_table()
        assert result.num_rows == 1
        assert result.column("id").to_pylist() == [1]

        cur.execute("DROP TABLE IF EXISTS hg_test_no_copy")


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


def test_get_statistics(hologres: dbapi.Connection) -> None:
    table_name = "hg_test_statistics"
    with hologres.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(
            f"CREATE TABLE {table_name} (id INT PRIMARY KEY, value TEXT)"
        )
        cur.execute(
            f"INSERT INTO {table_name} VALUES (1, 'a'), (2, 'b'), (3, 'c')"
        )
        cur.execute(f"ANALYZE {table_name}")

    reader = hologres.adbc_get_statistics(
        db_schema_filter="public",
        table_name_filter=table_name,
        approximate=True,
    )
    assert reader is not None
    table = reader.read_all()
    assert "catalog_name" in table.schema.names
    assert "catalog_db_schemas" in table.schema.names

    result_list = table.to_pylist()
    found = False
    for catalog in result_list:
        for schema in catalog["catalog_db_schemas"]:
            found = found or any(
                stat["table_name"] == table_name
                for stat in schema["db_schema_statistics"]
            )
    assert found, f"Expected statistics for '{table_name}'"

    with hologres.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_get_statistic_names(hologres: dbapi.Connection) -> None:
    reader = hologres.adbc_get_statistic_names()
    assert reader is not None
    table = reader.read_all()
    assert "statistic_name" in table.schema.names
    assert "statistic_key" in table.schema.names


# ---------------------------------------------------------------------------
# Temporal types — COPY mode round-trip
# ---------------------------------------------------------------------------


def test_ingest_date(hologres: dbapi.Connection) -> None:
    table = pyarrow.table(
        {
            "d": pyarrow.array(
                [datetime.date(2024, 1, 15), datetime.date(1999, 12, 31), None],
                type=pyarrow.date32(),
            ),
        }
    )
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_date")
        cur.adbc_ingest("hg_test_date", table, mode="create")

        cur.execute("SELECT d FROM hg_test_date ORDER BY d NULLS LAST")
        result = cur.fetch_arrow_table()
        vals = result.column("d").to_pylist()
        assert vals[0] == datetime.date(1999, 12, 31)
        assert vals[1] == datetime.date(2024, 1, 15)
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_date")


def test_ingest_timestamp(hologres: dbapi.Connection) -> None:
    ts1 = datetime.datetime(2024, 6, 15, 10, 30, 0)
    ts2 = datetime.datetime(2000, 1, 1, 0, 0, 0)
    table = pyarrow.table(
        {
            "ts": pyarrow.array(
                [ts1, ts2, None],
                type=pyarrow.timestamp("us"),
            ),
        }
    )
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_timestamp")
        cur.adbc_ingest("hg_test_timestamp", table, mode="create")

        cur.execute("SELECT ts FROM hg_test_timestamp ORDER BY ts NULLS LAST")
        result = cur.fetch_arrow_table()
        vals = result.column("ts").to_pylist()
        assert vals[0] == ts2
        assert vals[1] == ts1
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_timestamp")


def test_ingest_timestamptz(hologres: dbapi.Connection) -> None:
    tz = datetime.timezone.utc
    ts1 = datetime.datetime(2024, 6, 15, 10, 30, 0, tzinfo=tz)
    ts2 = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=tz)
    table = pyarrow.table(
        {
            "ts": pyarrow.array(
                [ts1, ts2, None],
                type=pyarrow.timestamp("us", tz="UTC"),
            ),
        }
    )
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_timestamptz")
        cur.adbc_ingest("hg_test_timestamptz", table, mode="create")

        cur.execute("SELECT ts FROM hg_test_timestamptz ORDER BY ts NULLS LAST")
        result = cur.fetch_arrow_table()
        vals = result.column("ts").to_pylist()
        # Hologres returns timestamptz as UTC
        assert vals[0].replace(tzinfo=tz) == ts2 or vals[0] == ts2
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_timestamptz")


def test_ingest_time(hologres: dbapi.Connection) -> None:
    t1 = datetime.time(10, 30, 0)
    t2 = datetime.time(23, 59, 59)
    table = pyarrow.table(
        {
            "t": pyarrow.array(
                [t1, t2, None],
                type=pyarrow.time64("us"),
            ),
        }
    )
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_time")
        cur.adbc_ingest("hg_test_time", table, mode="create")

        cur.execute("SELECT t FROM hg_test_time ORDER BY t NULLS LAST")
        result = cur.fetch_arrow_table()
        vals = result.column("t").to_pylist()
        assert vals[0] == t1
        assert vals[1] == t2
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_time")


@pytest.mark.xfail(
    reason="COPY writer does not yet support month_day_nano_interval type"
)
def test_ingest_interval(hologres: dbapi.Connection) -> None:
    table = pyarrow.table(
        {
            "iv": pyarrow.array(
                [
                    datetime.timedelta(days=30, seconds=3600),
                    datetime.timedelta(days=0),
                    None,
                ],
                type=pyarrow.month_day_nano_interval(),
            ),
        }
    )
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_interval")
        cur.adbc_ingest("hg_test_interval", table, mode="create")

        cur.execute("SELECT iv FROM hg_test_interval ORDER BY iv NULLS LAST")
        result = cur.fetch_arrow_table()
        assert result.num_rows == 3
        vals = result.column("iv").to_pylist()
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_interval")


# ---------------------------------------------------------------------------
# Numeric, JSON, Binary extended types — COPY mode
# ---------------------------------------------------------------------------


def test_ingest_decimal128(hologres: dbapi.Connection) -> None:
    """Decimal128 requires DDL first (auto-create does not map decimal128)."""
    table = pyarrow.table(
        {
            "d": pyarrow.array(
                [
                    decimal.Decimal("123.456"),
                    decimal.Decimal("-999.001"),
                    None,
                ],
                type=pyarrow.decimal128(10, 3),
            ),
        }
    )
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_dec128")
        cur.execute("CREATE TABLE hg_test_dec128 (d NUMERIC(10,3))")
        cur.adbc_ingest("hg_test_dec128", table, mode="append")

        cur.execute("SELECT d FROM hg_test_dec128 ORDER BY d NULLS LAST")
        result = cur.fetch_arrow_table()
        vals = result.column("d").to_pylist()
        # Hologres returns numeric as string via COPY reader
        assert vals[0] == "-999.001" or vals[0] == decimal.Decimal("-999.001")
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_dec128")


def test_ingest_decimal128_large(hologres: dbapi.Connection) -> None:
    """Large precision decimal128 requires DDL first."""
    table = pyarrow.table(
        {
            "d": pyarrow.array(
                [
                    decimal.Decimal("12345678901234567890.123456789012345678"),
                    decimal.Decimal("0.000000000000000001"),
                    None,
                ],
                type=pyarrow.decimal128(38, 18),
            ),
        }
    )
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_dec128_lg")
        cur.execute("CREATE TABLE hg_test_dec128_lg (d NUMERIC(38,18))")
        cur.adbc_ingest("hg_test_dec128_lg", table, mode="append")

        cur.execute("SELECT d FROM hg_test_dec128_lg ORDER BY d NULLS LAST")
        result = cur.fetch_arrow_table()
        assert result.num_rows == 3
        vals = result.column("d").to_pylist()
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_dec128_lg")


def test_ingest_json(hologres: dbapi.Connection) -> None:
    """Test JSON column via DDL + append."""
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_json")
        cur.execute("CREATE TABLE hg_test_json (id INT, data JSON)")

        table = pyarrow.table(
            {
                "id": pyarrow.array([1, 2, 3], type=pyarrow.int32()),
                "data": pyarrow.array(
                    ['{"key": "value"}', '{"num": 42}', None],
                    type=pyarrow.utf8(),
                ),
            }
        )
        cur.adbc_ingest("hg_test_json", table, mode="append")

        # JSON type cannot be used in ORDER BY, so order by id instead
        cur.execute("SELECT id, data FROM hg_test_json ORDER BY id")
        result = cur.fetch_arrow_table()
        vals = result.column("data").to_pylist()
        assert '"key"' in vals[0] and '"value"' in vals[0]
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_json")


@pytest.mark.xfail(
    reason="COPY binary protocol JSONB version byte not supported by Hologres"
)
def test_ingest_jsonb(hologres: dbapi.Connection) -> None:
    """Test JSONB column via DDL + append."""
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_jsonb")
        cur.execute("CREATE TABLE hg_test_jsonb (id INT, data JSONB)")

        table = pyarrow.table(
            {
                "id": pyarrow.array([1, 2, 3], type=pyarrow.int32()),
                "data": pyarrow.array(
                    ['{"name": "alice"}', '{"items": [1,2,3]}', None],
                    type=pyarrow.utf8(),
                ),
            }
        )
        cur.adbc_ingest("hg_test_jsonb", table, mode="append")

        cur.execute("SELECT id, data FROM hg_test_jsonb ORDER BY id")
        result = cur.fetch_arrow_table()
        vals = result.column("data").to_pylist()
        assert '"name"' in vals[0]
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_jsonb")


def test_ingest_large_string(hologres: dbapi.Connection) -> None:
    table = pyarrow.table(
        {
            "s": pyarrow.array(
                ["hello", "world", None], type=pyarrow.large_utf8()
            ),
        }
    )
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_large_str")
        cur.adbc_ingest("hg_test_large_str", table, mode="create")

        cur.execute(
            "SELECT s FROM hg_test_large_str ORDER BY s NULLS LAST"
        )
        result = cur.fetch_arrow_table()
        vals = result.column("s").to_pylist()
        assert vals[0] == "hello"
        assert vals[1] == "world"
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_large_str")


def test_ingest_large_binary(hologres: dbapi.Connection) -> None:
    table = pyarrow.table(
        {
            "b": pyarrow.array(
                [b"\xde\xad", b"\xbe\xef", None], type=pyarrow.large_binary()
            ),
        }
    )
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_large_bin")
        cur.adbc_ingest("hg_test_large_bin", table, mode="create")

        cur.execute(
            "SELECT b FROM hg_test_large_bin ORDER BY b NULLS LAST"
        )
        result = cur.fetch_arrow_table()
        vals = result.column("b").to_pylist()
        assert vals[0] == b"\xbe\xef"
        assert vals[1] == b"\xde\xad"
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_large_bin")


def test_ingest_fixed_size_binary(hologres: dbapi.Connection) -> None:
    table = pyarrow.table(
        {
            "b": pyarrow.array(
                [b"\x01\x02\x03\x04", b"\x05\x06\x07\x08", None],
                type=pyarrow.binary(4),
            ),
        }
    )
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_fsb")
        cur.adbc_ingest("hg_test_fsb", table, mode="create")

        cur.execute("SELECT b FROM hg_test_fsb ORDER BY b NULLS LAST")
        result = cur.fetch_arrow_table()
        vals = result.column("b").to_pylist()
        assert vals[0] == b"\x01\x02\x03\x04"
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_fsb")


def test_ingest_dictionary(hologres: dbapi.Connection) -> None:
    """Dictionary-encoded Arrow array should be transparently decoded."""
    indices = pyarrow.array([0, 1, 2, 0, None], type=pyarrow.int32())
    dictionary = pyarrow.array(["cat", "dog", "fish"])
    dict_arr = pyarrow.DictionaryArray.from_arrays(indices, dictionary)
    table = pyarrow.table({"animal": dict_arr})

    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_dict")
        cur.adbc_ingest("hg_test_dict", table, mode="create")

        cur.execute(
            "SELECT animal FROM hg_test_dict ORDER BY animal NULLS LAST"
        )
        result = cur.fetch_arrow_table()
        vals = result.column("animal").to_pylist()
        assert "cat" in vals
        assert "dog" in vals
        assert "fish" in vals
        assert vals[-1] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_dict")


# ---------------------------------------------------------------------------
# List / Array types — COPY mode
# ---------------------------------------------------------------------------


def test_ingest_list_int(hologres: dbapi.Connection) -> None:
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_list_int")
        cur.execute("CREATE TABLE hg_test_list_int (arr INT[])")

        table = pyarrow.table(
            {
                "arr": pyarrow.array(
                    [[1, 2, 3], [4, 5], None], type=pyarrow.list_(pyarrow.int32())
                ),
            }
        )
        cur.adbc_ingest("hg_test_list_int", table, mode="append")

        cur.execute("SELECT arr FROM hg_test_list_int ORDER BY arr NULLS LAST")
        result = cur.fetch_arrow_table()
        vals = result.column("arr").to_pylist()
        assert vals[0] == [1, 2, 3]
        assert vals[1] == [4, 5]
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_list_int")


def test_ingest_list_string(hologres: dbapi.Connection) -> None:
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_list_str")
        cur.execute("CREATE TABLE hg_test_list_str (arr TEXT[])")

        table = pyarrow.table(
            {
                "arr": pyarrow.array(
                    [["a", "b"], ["c"], None],
                    type=pyarrow.list_(pyarrow.utf8()),
                ),
            }
        )
        cur.adbc_ingest("hg_test_list_str", table, mode="append")

        cur.execute(
            "SELECT arr FROM hg_test_list_str ORDER BY arr NULLS LAST"
        )
        result = cur.fetch_arrow_table()
        vals = result.column("arr").to_pylist()
        assert vals[0] == ["a", "b"]
        assert vals[1] == ["c"]
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_list_str")


def test_ingest_list_float(hologres: dbapi.Connection) -> None:
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_list_flt")
        cur.execute("CREATE TABLE hg_test_list_flt (arr FLOAT8[])")

        table = pyarrow.table(
            {
                "arr": pyarrow.array(
                    [[1.1, 2.2], [3.3], None],
                    type=pyarrow.list_(pyarrow.float64()),
                ),
            }
        )
        cur.adbc_ingest("hg_test_list_flt", table, mode="append")

        cur.execute(
            "SELECT arr FROM hg_test_list_flt ORDER BY arr NULLS LAST"
        )
        result = cur.fetch_arrow_table()
        vals = result.column("arr").to_pylist()
        assert len(vals[0]) == 2
        assert abs(vals[0][0] - 1.1) < 0.001
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_list_flt")


def test_ingest_fixed_size_list(hologres: dbapi.Connection) -> None:
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_fsl")
        cur.execute("CREATE TABLE hg_test_fsl (arr INT[])")

        table = pyarrow.table(
            {
                "arr": pyarrow.array(
                    [[1, 2, 3], [4, 5, 6], None],
                    type=pyarrow.list_(pyarrow.int32(), 3),
                ),
            }
        )
        cur.adbc_ingest("hg_test_fsl", table, mode="append")

        cur.execute("SELECT arr FROM hg_test_fsl ORDER BY arr NULLS LAST")
        result = cur.fetch_arrow_table()
        vals = result.column("arr").to_pylist()
        assert vals[0] == [1, 2, 3]
        assert vals[1] == [4, 5, 6]
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_fsl")


# ---------------------------------------------------------------------------
# Stage mode — type coverage
# ---------------------------------------------------------------------------


def test_stage_temporal_types(hologres: dbapi.Connection) -> None:
    """Stage mode: date, timestamp, and timestamptz round-trip.

    The C driver converts Arrow types for Hologres Stage compatibility:
    - date32 (DateDay) is used as-is for DATE
    - timestamp[us] (TimeStampMicro) is used as-is for TIMESTAMP
    - timestamp[us, tz=UTC] is converted to date64[ms] (DateMilli) for TIMESTAMPTZ
    """
    table_name = "hg_test_stage_temporal"
    with hologres.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(
            f"CREATE TABLE {table_name} "
            f"(d DATE, ts TIMESTAMP, tstz TIMESTAMPTZ)"
        )

        table = pyarrow.table(
            {
                "d": pyarrow.array(
                    [datetime.date(2024, 1, 15), datetime.date(2000, 6, 1)],
                    type=pyarrow.date32(),
                ),
                "ts": pyarrow.array(
                    [
                        datetime.datetime(2024, 1, 1, 12, 0, 0),
                        datetime.datetime(2000, 6, 15, 8, 30, 0),
                    ],
                    type=pyarrow.timestamp("us"),
                ),
                "tstz": pyarrow.array(
                    [
                        datetime.datetime(2024, 3, 20, 10, 0, 0),
                        datetime.datetime(2000, 12, 31, 23, 59, 59),
                    ],
                    type=pyarrow.timestamp("us", tz="UTC"),
                ),
            }
        )
        cur.adbc_statement.set_options(
            **{StatementOptions.INGEST_MODE.value: "stage"}
        )
        cur.adbc_ingest(table_name, table, mode="append")

        cur.execute(f"SELECT d, ts, tstz FROM {table_name} ORDER BY ts")
        result = cur.fetch_arrow_table()
        assert result.num_rows == 2

        d_vals = result.column("d").to_pylist()
        assert datetime.date(2000, 6, 1) in d_vals
        assert datetime.date(2024, 1, 15) in d_vals

        ts_vals = result.column("ts").to_pylist()
        assert ts_vals[0] == datetime.datetime(2000, 6, 15, 8, 30, 0)
        assert ts_vals[1] == datetime.datetime(2024, 1, 1, 12, 0, 0)

        tstz_vals = result.column("tstz").to_pylist()
        assert len(tstz_vals) == 2

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_stage_numeric_types(hologres: dbapi.Connection) -> None:
    """Stage mode: int16, int64, float32, float64 round-trip.

    Note: NUMERIC/decimal128 is not supported by Hologres Stage import.
    """
    table_name = "hg_test_stage_numeric"
    with hologres.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(
            f"CREATE TABLE {table_name} "
            f"(i SMALLINT, b BIGINT, f REAL, d DOUBLE PRECISION)"
        )

        table = pyarrow.table(
            {
                "i": pyarrow.array([1, -32000], type=pyarrow.int16()),
                "b": pyarrow.array([100000, -100000], type=pyarrow.int64()),
                "f": pyarrow.array([1.5, -2.5], type=pyarrow.float32()),
                "d": pyarrow.array([3.14, -2.71], type=pyarrow.float64()),
            }
        )
        cur.adbc_statement.set_options(
            **{StatementOptions.INGEST_MODE.value: "stage"}
        )
        cur.adbc_ingest(table_name, table, mode="append")

        cur.execute(f"SELECT i, b, f, d FROM {table_name} ORDER BY i")
        result = cur.fetch_arrow_table()
        assert result.num_rows == 2
        ints = result.column("i").to_pylist()
        assert -32000 in ints
        assert 1 in ints
        bigs = result.column("b").to_pylist()
        assert 100000 in bigs

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.xfail(
    reason="Hologres Stage/EXTERNAL_FILES does not support JSON/JSONB types"
)
def test_stage_json_types(hologres: dbapi.Connection) -> None:
    """Stage mode: JSON and JSONB via DDL + Stage append."""
    table_name = "hg_test_stage_json"
    with hologres.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(
            f"CREATE TABLE {table_name} (j JSON, jb JSONB)"
        )

        table = pyarrow.table(
            {
                "j": pyarrow.array(
                    ['{"a": 1}', '{"b": 2}'], type=pyarrow.utf8()
                ),
                "jb": pyarrow.array(
                    ['{"c": 3}', '{"d": 4}'], type=pyarrow.utf8()
                ),
            }
        )
        cur.adbc_statement.set_options(
            **{StatementOptions.INGEST_MODE.value: "stage"}
        )
        cur.adbc_ingest(table_name, table, mode="append")

        cur.execute(f"SELECT j, jb FROM {table_name} ORDER BY j")
        result = cur.fetch_arrow_table()
        assert result.num_rows == 2
        j_vals = result.column("j").to_pylist()
        assert any('"a"' in v for v in j_vals)

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_stage_list_types(hologres: dbapi.Connection) -> None:
    """Stage mode: list<int32>, list<utf8> round-trip."""
    table_name = "hg_test_stage_list"
    with hologres.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(
            f"CREATE TABLE {table_name} (ints INT[], strs TEXT[])"
        )

        table = pyarrow.table(
            {
                "ints": pyarrow.array(
                    [[1, 2], [3, 4, 5]],
                    type=pyarrow.list_(pyarrow.int32()),
                ),
                "strs": pyarrow.array(
                    [["a", "b"], ["c"]],
                    type=pyarrow.list_(pyarrow.utf8()),
                ),
            }
        )
        cur.adbc_statement.set_options(
            **{StatementOptions.INGEST_MODE.value: "stage"}
        )
        cur.adbc_ingest(table_name, table, mode="append")

        cur.execute(f"SELECT ints, strs FROM {table_name} ORDER BY ints")
        result = cur.fetch_arrow_table()
        assert result.num_rows == 2
        ints_vals = result.column("ints").to_pylist()
        assert [1, 2] in ints_vals
        assert [3, 4, 5] in ints_vals

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_stage_binary_types(hologres: dbapi.Connection) -> None:
    """Stage mode: binary and large_binary round-trip.

    The C driver converts large_binary to binary for Hologres Stage compatibility.
    """
    table_name = "hg_test_stage_bin"
    with hologres.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(
            f"CREATE TABLE {table_name} (b1 BYTEA, b2 BYTEA)"
        )

        table = pyarrow.table(
            {
                "b1": pyarrow.array(
                    [b"\x01\x02", b"\x03\x04"], type=pyarrow.binary()
                ),
                "b2": pyarrow.array(
                    [b"\xaa\xbb", b"\xcc\xdd"], type=pyarrow.large_binary()
                ),
            }
        )
        cur.adbc_statement.set_options(
            **{StatementOptions.INGEST_MODE.value: "stage"}
        )
        cur.adbc_ingest(table_name, table, mode="append")

        cur.execute(f"SELECT b1, b2 FROM {table_name} ORDER BY b1")
        result = cur.fetch_arrow_table()
        assert result.num_rows == 2
        b1_vals = result.column("b1").to_pylist()
        assert b"\x01\x02" in b1_vals

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_stage_large_batch(hologres: dbapi.Connection) -> None:
    """Stage mode: 10000 rows to verify chunking and parallel upload."""
    import numpy as np

    table_name = "hg_test_stage_large"
    with hologres.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(
            f"CREATE TABLE {table_name} (id INT, val DOUBLE PRECISION)"
        )

        table = pyarrow.table(
            {
                "id": pyarrow.array(
                    np.arange(10000, dtype=np.int32())
                ),
                "val": pyarrow.array(
                    np.random.rand(10000).astype(np.float64)
                ),
            }
        )
        cur.adbc_statement.set_options(
            **{StatementOptions.INGEST_MODE.value: "stage"}
        )
        cur.adbc_ingest(table_name, table, mode="append")

        cur.execute(f"SELECT count(*) FROM {table_name}")
        assert cur.fetchone() == (10000,)

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_stage_null_handling(hologres: dbapi.Connection) -> None:
    """Stage mode: verify NULL handling behavior.

    Hologres Stage import converts Arrow NULLs to type-specific default values
    (int→0, float→0.0, string→"", bool→false). This test documents that
    behavior and verifies the non-null values are correct.
    """
    table_name = "hg_test_stage_nulls"
    with hologres.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(
            f"CREATE TABLE {table_name} "
            f"(i INT, f DOUBLE PRECISION, s TEXT, b BOOL)"
        )

        table = pyarrow.table(
            {
                "i": pyarrow.array([1, None, 3], type=pyarrow.int32()),
                "f": pyarrow.array([1.0, None, 3.0], type=pyarrow.float64()),
                "s": pyarrow.array(["a", None, "c"], type=pyarrow.utf8()),
                "b": pyarrow.array([True, None, False], type=pyarrow.bool_()),
            }
        )
        cur.adbc_statement.set_options(
            **{StatementOptions.INGEST_MODE.value: "stage"}
        )
        cur.adbc_ingest(table_name, table, mode="append")

        cur.execute(f"SELECT i, f, s, b FROM {table_name} ORDER BY i")
        result = cur.fetch_arrow_table()
        assert result.num_rows == 3

        # Verify non-null values are correctly ingested
        i_vals = result.column("i").to_pylist()
        assert 1 in i_vals
        assert 3 in i_vals

        f_vals = result.column("f").to_pylist()
        assert 1.0 in f_vals
        assert 3.0 in f_vals

        s_vals = result.column("s").to_pylist()
        assert "a" in s_vals
        assert "c" in s_vals

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


# ---------------------------------------------------------------------------
# Operational tests
# ---------------------------------------------------------------------------


def test_ingest_error_nonexistent_table(hologres: dbapi.Connection) -> None:
    """Appending to a non-existent table should raise an error."""
    table = pyarrow.table({"x": pyarrow.array([1], type=pyarrow.int32())})
    with hologres.cursor() as cur:
        with pytest.raises(Exception):
            cur.adbc_ingest(
                "hg_table_absolutely_does_not_exist_xyz", table, mode="append"
            )


def test_ingest_error_schema_mismatch(hologres: dbapi.Connection) -> None:
    """Ingesting data with wrong column names should raise an error."""
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_mismatch")
        cur.execute("CREATE TABLE hg_test_mismatch (id INT, name TEXT)")

        wrong_schema = pyarrow.table(
            {"wrong_col": pyarrow.array([1], type=pyarrow.int32())}
        )
        with pytest.raises(Exception):
            cur.adbc_ingest("hg_test_mismatch", wrong_schema, mode="append")

        cur.execute("DROP TABLE IF EXISTS hg_test_mismatch")


def test_cursor_reuse(hologres: dbapi.Connection) -> None:
    """Same cursor should handle multiple sequential queries."""
    with hologres.cursor() as cur:
        cur.execute("SELECT 1 AS a")
        assert cur.fetchone() == (1,)

        cur.execute("SELECT 2 AS b")
        assert cur.fetchone() == (2,)

        cur.execute("SELECT 'hello' AS c")
        assert cur.fetchone() == ("hello",)


def test_ingest_record_batches(hologres: dbapi.Connection) -> None:
    """Ingest using RecordBatchReader as input."""
    schema = pyarrow.schema(
        [("id", pyarrow.int32()), ("name", pyarrow.utf8())]
    )
    batch1 = pyarrow.record_batch(
        [pyarrow.array([1, 2]), pyarrow.array(["a", "b"])], schema=schema
    )
    batch2 = pyarrow.record_batch(
        [pyarrow.array([3, 4]), pyarrow.array(["c", "d"])], schema=schema
    )
    reader = pyarrow.RecordBatchReader.from_batches(schema, [batch1, batch2])

    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_rb")
        cur.adbc_ingest("hg_test_rb", reader, mode="create")

        cur.execute("SELECT count(*) FROM hg_test_rb")
        assert cur.fetchone() == (4,)

        cur.execute("SELECT id, name FROM hg_test_rb ORDER BY id")
        result = cur.fetch_arrow_table()
        assert result.column("id").to_pylist() == [1, 2, 3, 4]

        cur.execute("DROP TABLE IF EXISTS hg_test_rb")


def test_on_conflict_none_error(hologres: dbapi.Connection) -> None:
    """Default ON_CONFLICT (none) should error on primary key conflict."""
    table_name = "hg_test_conflict_none"
    _make_pk_table(hologres, table_name)

    initial = pyarrow.table(
        {
            "id": pyarrow.array([1, 2], type=pyarrow.int32()),
            "value": pyarrow.array(["a", "b"]),
        }
    )
    duplicate = pyarrow.table(
        {
            "id": pyarrow.array([2, 3], type=pyarrow.int32()),
            "value": pyarrow.array(["dup", "c"]),
        }
    )

    with hologres.cursor() as cur:
        cur.adbc_ingest(table_name, initial, mode="append")

        with pytest.raises(Exception):
            cur.adbc_ingest(table_name, duplicate, mode="append")

        cur.execute(f"DROP TABLE IF EXISTS {table_name}")


def test_fetch_df(hologres: dbapi.Connection) -> None:
    """fetch_df() should return a pandas DataFrame."""
    pd = pytest.importorskip("pandas")

    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_fetch_df")
        cur.execute("CREATE TABLE hg_test_fetch_df (id INT, name TEXT)")
        cur.execute(
            "INSERT INTO hg_test_fetch_df VALUES (1, 'alice'), (2, 'bob')"
        )

        cur.execute("SELECT id, name FROM hg_test_fetch_df ORDER BY id")
        df = cur.fetch_df()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df["id"]) == [1, 2]
        assert list(df["name"]) == ["alice", "bob"]

        cur.execute("DROP TABLE IF EXISTS hg_test_fetch_df")


def test_multiple_result_sets(hologres: dbapi.Connection) -> None:
    """Sequential execute + fetch should return independent results."""
    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_multi_rs")
        cur.execute("CREATE TABLE hg_test_multi_rs (id INT)")
        cur.execute(
            "INSERT INTO hg_test_multi_rs VALUES (1), (2), (3)"
        )

        cur.execute("SELECT id FROM hg_test_multi_rs WHERE id <= 2 ORDER BY id")
        r1 = cur.fetch_arrow_table()
        assert r1.num_rows == 2

        cur.execute("SELECT id FROM hg_test_multi_rs WHERE id > 2 ORDER BY id")
        r2 = cur.fetch_arrow_table()
        assert r2.num_rows == 1
        assert r2.column("id").to_pylist() == [3]

        cur.execute("DROP TABLE IF EXISTS hg_test_multi_rs")


def test_empty_table_ingest(hologres: dbapi.Connection) -> None:
    """Ingesting an empty table (0 rows) should succeed."""
    schema = pyarrow.schema([("id", pyarrow.int32()), ("val", pyarrow.utf8())])
    empty = pyarrow.table(
        {"id": pyarrow.array([], type=pyarrow.int32()),
         "val": pyarrow.array([], type=pyarrow.utf8())},
    )

    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_empty")
        cur.adbc_ingest("hg_test_empty", empty, mode="create")

        cur.execute("SELECT count(*) FROM hg_test_empty")
        assert cur.fetchone() == (0,)

        cur.execute("DROP TABLE IF EXISTS hg_test_empty")


def test_ingest_large_strings(hologres: dbapi.Connection) -> None:
    """Round-trip large string values (>64KB)."""
    large_str = "x" * 100_000  # 100 KB string
    table = pyarrow.table(
        {"s": pyarrow.array([large_str, "short", None], type=pyarrow.utf8())}
    )

    with hologres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS hg_test_large_strs")
        cur.adbc_ingest("hg_test_large_strs", table, mode="create")

        cur.execute(
            "SELECT s FROM hg_test_large_strs ORDER BY length(s) DESC NULLS LAST"
        )
        result = cur.fetch_arrow_table()
        vals = result.column("s").to_pylist()
        assert len(vals[0]) == 100_000
        assert vals[1] == "short"
        assert vals[2] is None

        cur.execute("DROP TABLE IF EXISTS hg_test_large_strs")
