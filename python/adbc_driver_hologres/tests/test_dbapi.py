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
