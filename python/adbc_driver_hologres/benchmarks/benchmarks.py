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

"""Hologres-specific benchmarks for the ADBC Hologres driver.

This module provides benchmarks comparing ADBC, asyncpg, psycopg2, and DuckDB
performance when connecting to Alibaba Cloud Hologres.

Hologres has specific limitations that these benchmarks account for:
- No transaction support (autocommit only)
- Automatic STREAM_MODE TRUE for COPY operations
- ON_CONFLICT option for bulk ingestion
- Stage mode for Arrow-based ingestion via internal temporary storage
- No interval type support in COPY binary mode
"""

import abc
import asyncio
import gc
import itertools
import os

import asyncpg
import duckdb
import pandas
import psycopg
import pyarrow
import sqlalchemy

import adbc_driver_hologres
import adbc_driver_hologres.dbapi


class HologresBenchmarkBase(abc.ABC):
    """Base class for Hologres benchmarks.

    Sets up connections to Hologres and validates that the target
    is actually a Hologres instance.
    """

    async_conn: asyncpg.Connection
    async_runner: asyncio.Runner
    conn: adbc_driver_hologres.dbapi.Connection
    duck: duckdb.DuckDBPyConnection
    sqlalchemy_connection: sqlalchemy.engine.base.Connection

    def setup(self, *args, **kwargs) -> None:
        self.uri = os.environ.get("ADBC_HOLOGRES_TEST_URI")
        if not self.uri:
            raise NotImplementedError("ADBC_HOLOGRES_TEST_URI not set")

        self.table = self._make_table_name(*args, **kwargs)

        # Set up ADBC connection and verify it's Hologres
        self.conn = adbc_driver_hologres.dbapi.connect(
            self.uri, autocommit=True
        )
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM information_schema.tables "
                "WHERE table_name = 'pg_database' LIMIT 1"
            )
            # Just verify connection works
            cursor.fetchone()

        # Set up asyncpg
        self.async_runner = asyncio.Runner()
        self.async_conn = self.async_runner.run(asyncpg.connect(dsn=self.uri))

        # Set up DuckDB with postgres_scanner
        # Note: postgres_scanner compatibility with Hologres may vary
        self.duck = duckdb.connect()
        try:
            self.duck.sql("INSTALL postgres_scanner")
            self.duck.sql("LOAD postgres_scanner")
            self.duck.sql(f"CALL postgres_attach('{self.uri}')")
        except Exception:
            # DuckDB postgres_scanner may not be fully compatible with Hologres
            self.duck = None

        # Set up SQLAlchemy/psycopg2
        uri = self.uri.replace("postgres://", "postgresql+psycopg2://")
        self.sqlalchemy_connection = sqlalchemy.create_engine(uri).connect()

    def teardown(self, *args, **kwargs) -> None:
        if hasattr(self, "async_runner"):
            self.async_runner.close()
        if hasattr(self, "conn"):
            self.conn.close()
        if hasattr(self, "sqlalchemy_connection"):
            self.sqlalchemy_connection.close()

    @abc.abstractmethod
    def _make_table_name(self, *args, **kwargs) -> str: ...

    def time_pandas_adbc(self, row_count: int, data_type: str) -> None:
        """Fetch data using ADBC and convert to Pandas DataFrame."""
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {self.table}")
            cursor.fetch_df()

    def time_pandas_adbc_arrow(self, row_count: int, data_type: str) -> None:
        """Fetch data using ADBC with arrow COPY format and convert to Pandas."""
        with self.conn.cursor() as cursor:
            cursor.adbc_statement.set_options(
                **{adbc_driver_hologres.StatementOptions.COPY_FORMAT.value: "arrow"}
            )
            cursor.execute(f"SELECT * FROM {self.table}")
            cursor.fetch_df()
            cursor.adbc_statement.set_options(
                **{adbc_driver_hologres.StatementOptions.COPY_FORMAT.value: "binary"}
            )

    def time_pandas_adbc_arrow_lz4(self, row_count: int, data_type: str) -> None:
        """Fetch data using ADBC with arrow_lz4 COPY format and convert to Pandas."""
        with self.conn.cursor() as cursor:
            cursor.adbc_statement.set_options(
                **{adbc_driver_hologres.StatementOptions.COPY_FORMAT.value: "arrow_lz4"}
            )
            cursor.execute(f"SELECT * FROM {self.table}")
            cursor.fetch_df()
            cursor.adbc_statement.set_options(
                **{adbc_driver_hologres.StatementOptions.COPY_FORMAT.value: "binary"}
            )

    def time_pandas_asyncpg(self, row_count: int, data_type: str) -> None:
        """Fetch data using asyncpg and convert to Pandas DataFrame."""
        records = self.async_runner.run(
            self.async_conn.fetch(f"SELECT * FROM {self.table}")
        )
        pandas.DataFrame(records)

    def time_pandas_psycopg2(self, row_count: int, data_type: str) -> None:
        """Fetch data using SQLAlchemy/psycopg2 and convert to Pandas DataFrame."""
        pandas.read_sql_table(self.table, self.sqlalchemy_connection)

    def time_pandas_duckdb(self, row_count: int, data_type: str) -> None:
        """Fetch data using DuckDB postgres_scanner and convert to Pandas DataFrame."""
        if self.duck is None:
            raise NotImplementedError("DuckDB postgres_scanner not available")
        self.duck.sql(f"SELECT * FROM {self.table}").fetchdf()


class HologresOneColumnSuite(HologresBenchmarkBase):
    """Benchmark fetching a single column of a given type from Hologres."""

    SETUP_QUERIES = [
        "DROP TABLE IF EXISTS {table_name}",
        "CREATE TABLE {table_name} (items {data_type})",
        """INSERT INTO {table_name} (items)
        SELECT generated :: {data_type}
        FROM GENERATE_SERIES(1, {row_count}) temp(generated)""",
    ]

    param_data = {
        "row_count": [10_000, 100_000, 1_000_000, 10_000_000],
        "data_type": ["INT", "BIGINT", "FLOAT", "DOUBLE PRECISION", "TEXT", "TEXT[]"],
    }

    param_names = list(param_data.keys())
    params = list(param_data.values())

    def setup_cache(self) -> None:
        """Create test tables with data.

        Note: Hologres doesn't support transactions, so we use autocommit mode.
        """
        self.uri = os.environ.get("ADBC_HOLOGRES_TEST_URI")
        if not self.uri:
            return

        # Use psycopg with autocommit for data preparation
        with psycopg.connect(self.uri, autocommit=True) as conn:
            with conn.cursor() as cursor:
                for row_count, data_type in itertools.product(*self.params):
                    table_name = self._make_table_name(row_count, data_type)
                    for query in self.SETUP_QUERIES:
                        try:
                            cursor.execute(
                                query.format(
                                    table_name=table_name,
                                    row_count=row_count,
                                    data_type=data_type,
                                )
                            )
                        except psycopg.Error:
                            # GENERATE_SERIES or type cast may fail
                            # (e.g. TEXT[] can't be cast from integer)
                            self._setup_with_adbc_ingest(
                                table_name, row_count, data_type
                            )
                            break

    def _setup_with_adbc_ingest(
        self, table_name: str, row_count: int, data_type: str
    ) -> None:
        """Fallback: set up table using ADBC bulk ingest."""
        import numpy as np

        conn = adbc_driver_hologres.dbapi.connect(
            self.uri, autocommit=True
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                cursor.execute(
                    f"CREATE TABLE {table_name} (items {data_type})"
                )

                # Generate data and ingest
                if data_type == "INT":
                    arr = pyarrow.array(np.arange(row_count, dtype=np.int32))
                elif data_type == "BIGINT":
                    arr = pyarrow.array(np.arange(row_count, dtype=np.int64))
                elif data_type in ("FLOAT", "REAL"):
                    arr = pyarrow.array(
                        np.arange(row_count, dtype=np.float32)
                    )
                elif data_type == "TEXT":
                    arr = pyarrow.array(
                        [f"row_{i}" for i in range(row_count)]
                    )
                elif data_type == "TEXT[]":
                    values = pyarrow.array(
                        [f"v{i}" for i in range(row_count * 3)]
                    )
                    offsets = pyarrow.array(
                        range(0, row_count * 3 + 1, 3),
                        type=pyarrow.int32(),
                    )
                    arr = pyarrow.ListArray.from_arrays(offsets, values)
                else:  # DOUBLE PRECISION
                    arr = pyarrow.array(
                        np.arange(row_count, dtype=np.float64)
                    )

                table = pyarrow.table({"items": arr})
                cursor.adbc_ingest(table_name, table, mode="append")
        finally:
            conn.close()

    def _make_table_name(self, row_count: int, data_type: str) -> str:
        return (
            f"holo_bench_{row_count}_"
            f"{data_type.replace(' ', '_').replace('[]', '_array')}"
        ).lower()


class HologresMultiColumnSuite(HologresBenchmarkBase):
    """Benchmark fetching multiple columns of a given type from Hologres."""

    SETUP_QUERIES = [
        "DROP TABLE IF EXISTS {table_name}",
        """
        CREATE TABLE {table_name} (
            a {data_type},
            b {data_type},
            c {data_type},
            d {data_type}
        )
        """,
        """
        INSERT INTO {table_name} (a, b, c, d)
        SELECT generated :: {data_type},
               generated :: {data_type},
               generated :: {data_type},
               generated :: {data_type}
        FROM GENERATE_SERIES(1, {row_count}) temp(generated)
        """,
    ]

    param_data = {
        "row_count": [10_000, 100_000, 1_000_000, 10_000_000],
        "data_type": ["INT", "BIGINT", "FLOAT", "DOUBLE PRECISION", "TEXT", "TEXT[]"],
    }

    param_names = list(param_data.keys())
    params = list(param_data.values())

    # TEXT[] at 10M rows (4 columns) exceeds memory during data generation,
    # causing OOM that kills the setup_cache subprocess and skips ALL benchmarks.
    _MAX_TEXT_ARRAY_ROWS = 1_000_000

    def setup_cache(self) -> None:
        """Create test tables with data."""
        self.uri = os.environ.get("ADBC_HOLOGRES_TEST_URI")
        if not self.uri:
            return

        with psycopg.connect(self.uri, autocommit=True) as conn:
            with conn.cursor() as cursor:
                for row_count, data_type in itertools.product(*self.params):
                    if (
                        data_type == "TEXT[]"
                        and row_count > self._MAX_TEXT_ARRAY_ROWS
                    ):
                        continue
                    table_name = self._make_table_name(row_count, data_type)
                    for query in self.SETUP_QUERIES:
                        try:
                            cursor.execute(
                                query.format(
                                    table_name=table_name,
                                    row_count=row_count,
                                    data_type=data_type,
                                )
                            )
                        except psycopg.Error:
                            # GENERATE_SERIES or type cast may fail
                            self._setup_with_adbc_ingest(
                                table_name, row_count, data_type
                            )
                            break

    def _setup_with_adbc_ingest(
        self, table_name: str, row_count: int, data_type: str
    ) -> None:
        """Fallback: set up table using ADBC bulk ingest."""
        import numpy as np

        conn = adbc_driver_hologres.dbapi.connect(
            self.uri, autocommit=True
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                cursor.execute(
                    f"CREATE TABLE {table_name} "
                    f"(a {data_type}, b {data_type}, "
                    f"c {data_type}, d {data_type})"
                )

                if data_type == "INT":
                    arr = pyarrow.array(np.arange(row_count, dtype=np.int32))
                elif data_type == "BIGINT":
                    arr = pyarrow.array(np.arange(row_count, dtype=np.int64))
                elif data_type in ("FLOAT", "REAL"):
                    arr = pyarrow.array(
                        np.arange(row_count, dtype=np.float32)
                    )
                elif data_type == "TEXT":
                    arr = pyarrow.array(
                        [f"row_{i}" for i in range(row_count)]
                    )
                elif data_type == "TEXT[]":
                    values = pyarrow.array(
                        [f"v{i}" for i in range(row_count * 3)]
                    )
                    offsets = pyarrow.array(
                        range(0, row_count * 3 + 1, 3),
                        type=pyarrow.int32(),
                    )
                    arr = pyarrow.ListArray.from_arrays(offsets, values)
                else:  # DOUBLE PRECISION
                    arr = pyarrow.array(
                        np.arange(row_count, dtype=np.float64)
                    )

                table = pyarrow.table(
                    {"a": arr, "b": arr, "c": arr, "d": arr}
                )
                cursor.adbc_ingest(table_name, table, mode="append")
        finally:
            conn.close()

    def setup(self, row_count: int, data_type: str) -> None:
        if data_type == "TEXT[]" and row_count > self._MAX_TEXT_ARRAY_ROWS:
            raise NotImplementedError(
                f"TEXT[] not benchmarked at {row_count} rows (OOM)"
            )
        super().setup(row_count, data_type)

    def _make_table_name(self, row_count: int, data_type: str) -> str:
        return (
            f"holo_bench_multi_{row_count}_"
            f"{data_type.replace(' ', '_').replace('[]', '_array')}"
        ).lower()


class HologresIngestSuite:
    """Benchmark bulk ingestion to Hologres using ADBC vs other methods.

    Tests ADBC COPY and Stage performance with different ON_CONFLICT modes.
    """

    _MODES = [
        "adbc",
        "adbc_ignore",
        "adbc_update",
        "stage",
        "stage_ignore",
        "stage_update",
        "asyncpg",
        "psycopg2",
    ]

    timeout = 600  # ingest benchmarks need more time (asyncpg is slow)

    param_data = {
        "row_count": [10_000, 100_000, 1_000_000],
    }

    param_names = list(param_data.keys())
    params = list(param_data.values())

    def setup_cache(self) -> None:
        """Create all target tables once before any benchmarks run."""
        uri = os.environ.get("ADBC_HOLOGRES_TEST_URI")
        if not uri:
            return

        with psycopg.connect(uri, autocommit=True) as conn:
            with conn.cursor() as cursor:
                for (row_count,) in itertools.product(*self.params):
                    table_base = f"holo_ingest_bench_{row_count}"
                    for mode in self._MODES:
                        table_name = f"{table_base}_{mode}"
                        cursor.execute(
                            f"DROP TABLE IF EXISTS {table_name}"
                        )
                        cursor.execute(
                            f"CREATE TABLE {table_name} "
                            f"(id BIGINT PRIMARY KEY, "
                            f"value DOUBLE PRECISION, name TEXT)"
                        )

    def setup(self, row_count: int) -> None:
        self.uri = os.environ.get("ADBC_HOLOGRES_TEST_URI")
        if not self.uri:
            raise NotImplementedError("ADBC_HOLOGRES_TEST_URI not set")

        gc.collect()

        self.row_count = row_count
        self.table_base = f"holo_ingest_bench_{row_count}"

        # Prepare test data as PyArrow table
        import numpy as np

        self.test_data = pyarrow.table(
            {
                "id": pyarrow.array(np.arange(row_count, dtype=np.int64)),
                "value": pyarrow.array(
                    np.random.randn(row_count).astype(np.float64)
                ),
                "name": pyarrow.array(
                    [f"row_{i}" for i in range(row_count)]
                ),
            }
        )

        self._adbc_conn = adbc_driver_hologres.dbapi.connect(
            self.uri, autocommit=True
        )

        # Pre-convert test data for asyncpg and psycopg2
        ids = self.test_data.column("id").to_pylist()
        values = self.test_data.column("value").to_pylist()
        names = self.test_data.column("name").to_pylist()
        self._asyncpg_data = list(zip(ids, values, names))
        self._psycopg2_data = [
            {"id": i, "value": v, "name": n}
            for i, v, n in zip(ids, values, names)
        ]

    def teardown(self, row_count: int) -> None:
        if hasattr(self, "_adbc_conn"):
            self._adbc_conn.close()
        if hasattr(self, "test_data"):
            del self.test_data
        gc.collect()

    def time_ingest_adbc(self, row_count: int) -> None:
        """Benchmark ADBC bulk ingest without ON_CONFLICT."""
        table_name = f"{self.table_base}_adbc"
        with self._adbc_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.adbc_ingest(
                table_name, self.test_data, mode="create_append"
            )

    def time_ingest_adbc_on_conflict_ignore(self, row_count: int) -> None:
        """Benchmark ADBC bulk ingest with ON_CONFLICT='ignore'."""
        table_name = f"{self.table_base}_adbc_ignore"
        with self._adbc_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.adbc_statement.set_options(
                **{adbc_driver_hologres.StatementOptions.ON_CONFLICT.value: "ignore"}
            )
            cursor.adbc_ingest(
                table_name, self.test_data, mode="create_append"
            )

    def time_ingest_adbc_on_conflict_update(self, row_count: int) -> None:
        """Benchmark ADBC bulk ingest with ON_CONFLICT='update'."""
        table_name = f"{self.table_base}_adbc_update"
        with self._adbc_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.adbc_statement.set_options(
                **{adbc_driver_hologres.StatementOptions.ON_CONFLICT.value: "update"}
            )
            cursor.adbc_ingest(
                table_name, self.test_data, mode="create_append"
            )

    def time_ingest_adbc_stage(self, row_count: int) -> None:
        """Benchmark ADBC bulk ingest using Hologres Stage mode."""
        table_name = f"{self.table_base}_stage"
        with self._adbc_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.adbc_statement.set_options(
                **{adbc_driver_hologres.StatementOptions.INGEST_MODE.value: "stage"}
            )
            cursor.adbc_ingest(
                table_name, self.test_data, mode="create_append"
            )

    def time_ingest_adbc_stage_on_conflict_ignore(
        self, row_count: int
    ) -> None:
        """Benchmark ADBC Stage mode ingest with ON_CONFLICT='ignore'."""
        table_name = f"{self.table_base}_stage_ignore"
        with self._adbc_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.adbc_statement.set_options(
                **{
                    adbc_driver_hologres.StatementOptions.INGEST_MODE.value: "stage",
                    adbc_driver_hologres.StatementOptions.ON_CONFLICT.value: "ignore",
                }
            )
            cursor.adbc_ingest(
                table_name, self.test_data, mode="create_append"
            )

    def time_ingest_adbc_stage_on_conflict_update(
        self, row_count: int
    ) -> None:
        """Benchmark ADBC Stage mode ingest with ON_CONFLICT='update'."""
        table_name = f"{self.table_base}_stage_update"
        with self._adbc_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.adbc_statement.set_options(
                **{
                    adbc_driver_hologres.StatementOptions.INGEST_MODE.value: "stage",
                    adbc_driver_hologres.StatementOptions.ON_CONFLICT.value: "update",
                }
            )
            cursor.adbc_ingest(
                table_name, self.test_data, mode="create_append"
            )

    # asyncpg/psycopg2 executemany are row-by-row INSERT (~800 rows/s on
    # Hologres), making 1M rows infeasible.  Override params to skip 1M.
    _small_row_params = [[10_000, 100_000]]

    def time_ingest_asyncpg(self, row_count: int) -> None:
        """Benchmark asyncpg executemany INSERT."""
        table_name = f"{self.table_base}_asyncpg"

        async def _ingest():
            conn = await asyncpg.connect(dsn=self.uri)
            try:
                await conn.execute(f"TRUNCATE TABLE {table_name}")
                await conn.executemany(
                    f"INSERT INTO {table_name} (id, value, name) "
                    f"VALUES ($1, $2, $3)",
                    self._asyncpg_data,
                )
            finally:
                await conn.close()

        asyncio.run(_ingest())

    time_ingest_asyncpg.params = _small_row_params

    def time_ingest_psycopg2(self, row_count: int) -> None:
        """Benchmark psycopg2 executemany INSERT."""
        table_name = f"{self.table_base}_psycopg2"
        uri = self.uri.replace("postgres://", "postgresql+psycopg2://")
        engine = sqlalchemy.create_engine(uri)
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text(f"TRUNCATE TABLE {table_name}"))
            conn.commit()
            stmt = sqlalchemy.text(
                f"INSERT INTO {table_name} (id, value, name) "
                f"VALUES (:id, :value, :name)"
            )
            conn.execute(stmt, self._psycopg2_data)
            conn.commit()

    time_ingest_psycopg2.params = _small_row_params


class HologresVectorIngestSuite:
    """Benchmark vector data ingestion to Hologres using ADBC vs other methods.

    Tests ADBC COPY and Stage performance with FLOAT4[] vector columns
    (512-dim and 1024-dim) using different ON_CONFLICT modes.

    Hologres stores vectors as FLOAT4[] (PostgreSQL array of float4).
    ADBC maps pyarrow.fixed_size_list(float32(), N) to FLOAT4 ARRAY
    in both COPY binary and Stage (Arrow IPC) modes.
    """

    _MODES = [
        "adbc",
        "adbc_ignore",
        "adbc_update",
        "stage",
        "stage_ignore",
        "stage_update",
        "asyncpg",
        "psycopg2",
    ]

    timeout = 1800  # vector ingest benchmarks need more time (large data)

    param_data = {
        "vector_dim": [512, 1024],
        "row_count": [10_000, 100_000, 1_000_000, 10_000_000],
    }

    param_names = list(param_data.keys())
    params = list(param_data.values())

    def setup_cache(self) -> None:
        """Create all target tables once before any benchmarks run."""
        uri = os.environ.get("ADBC_HOLOGRES_TEST_URI")
        if not uri:
            return

        with psycopg.connect(uri, autocommit=True) as conn:
            with conn.cursor() as cursor:
                for vector_dim, row_count in itertools.product(
                    *self.params
                ):
                    table_base = (
                        f"holo_vec_ingest_{vector_dim}d_{row_count}"
                    )
                    for mode in self._MODES:
                        table_name = f"{table_base}_{mode}"
                        cursor.execute(
                            f"DROP TABLE IF EXISTS {table_name}"
                        )
                        cursor.execute(
                            f"CREATE TABLE {table_name} "
                            f"(id BIGINT PRIMARY KEY, "
                            f"embedding FLOAT4[])"
                        )

    def setup(self, vector_dim: int, row_count: int) -> None:
        self.uri = os.environ.get("ADBC_HOLOGRES_TEST_URI")
        if not self.uri:
            raise NotImplementedError("ADBC_HOLOGRES_TEST_URI not set")

        gc.collect()

        self.vector_dim = vector_dim
        self.row_count = row_count
        self.table_base = f"holo_vec_ingest_{vector_dim}d_{row_count}"

        # Prepare test data as PyArrow table
        import numpy as np

        # Generate random float32 vector data using fixed_size_list
        # which maps to FLOAT4[] in Hologres DDL.
        rng = np.random.default_rng()
        embedding_values = rng.standard_normal(
            row_count * vector_dim, dtype=np.float32
        )
        embedding_array = pyarrow.FixedSizeListArray.from_arrays(
            pyarrow.array(embedding_values, type=pyarrow.float32()),
            list_size=vector_dim,
        )

        self.test_data = pyarrow.table(
            {
                "id": pyarrow.array(np.arange(row_count, dtype=np.int64)),
                "embedding": embedding_array,
            }
        )

        self._adbc_conn = adbc_driver_hologres.dbapi.connect(
            self.uri, autocommit=True
        )

        # Pre-convert test data for asyncpg and psycopg2 only at small
        # row counts (to_pylist() on large vector data is memory-intensive)
        if row_count <= 100_000:
            ids = self.test_data.column("id").to_pylist()
            embeddings = self.test_data.column("embedding").to_pylist()
            self._asyncpg_data = list(zip(ids, embeddings))
            self._psycopg2_data = [
                {"id": i, "embedding": e}
                for i, e in zip(ids, embeddings)
            ]
        else:
            self._asyncpg_data = None
            self._psycopg2_data = None

    def teardown(self, vector_dim: int, row_count: int) -> None:
        if hasattr(self, "_adbc_conn"):
            self._adbc_conn.close()
        if hasattr(self, "test_data"):
            del self.test_data
        gc.collect()

    # COPY binary builds the entire data stream in memory; at 10M rows with
    # 512/1024-dim float32 vectors this exceeds available memory.
    # Stage mode uses chunked Arrow IPC and handles 10M rows fine.
    _copy_row_params = [[512, 1024], [10_000, 100_000, 1_000_000]]

    def time_ingest_adbc(self, vector_dim: int, row_count: int) -> None:
        """Benchmark ADBC COPY ingest of vector data."""
        table_name = f"{self.table_base}_adbc"
        with self._adbc_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.adbc_ingest(
                table_name, self.test_data, mode="create_append"
            )

    time_ingest_adbc.params = _copy_row_params

    def time_ingest_adbc_on_conflict_ignore(
        self, vector_dim: int, row_count: int
    ) -> None:
        """Benchmark ADBC COPY ingest of vector data with ON_CONFLICT='ignore'."""
        table_name = f"{self.table_base}_adbc_ignore"
        with self._adbc_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.adbc_statement.set_options(
                **{adbc_driver_hologres.StatementOptions.ON_CONFLICT.value: "ignore"}
            )
            cursor.adbc_ingest(
                table_name, self.test_data, mode="create_append"
            )

    time_ingest_adbc_on_conflict_ignore.params = _copy_row_params

    def time_ingest_adbc_on_conflict_update(
        self, vector_dim: int, row_count: int
    ) -> None:
        """Benchmark ADBC COPY ingest of vector data with ON_CONFLICT='update'."""
        table_name = f"{self.table_base}_adbc_update"
        with self._adbc_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.adbc_statement.set_options(
                **{adbc_driver_hologres.StatementOptions.ON_CONFLICT.value: "update"}
            )
            cursor.adbc_ingest(
                table_name, self.test_data, mode="create_append"
            )

    time_ingest_adbc_on_conflict_update.params = _copy_row_params

    def time_ingest_adbc_stage(
        self, vector_dim: int, row_count: int
    ) -> None:
        """Benchmark ADBC Stage mode ingest of vector data."""
        table_name = f"{self.table_base}_stage"
        with self._adbc_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.adbc_statement.set_options(
                **{adbc_driver_hologres.StatementOptions.INGEST_MODE.value: "stage"}
            )
            cursor.adbc_ingest(
                table_name, self.test_data, mode="create_append"
            )

    def time_ingest_adbc_stage_on_conflict_ignore(
        self, vector_dim: int, row_count: int
    ) -> None:
        """Benchmark ADBC Stage mode ingest of vector data with ON_CONFLICT='ignore'."""
        table_name = f"{self.table_base}_stage_ignore"
        with self._adbc_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.adbc_statement.set_options(
                **{
                    adbc_driver_hologres.StatementOptions.INGEST_MODE.value: "stage",
                    adbc_driver_hologres.StatementOptions.ON_CONFLICT.value: "ignore",
                }
            )
            cursor.adbc_ingest(
                table_name, self.test_data, mode="create_append"
            )

    def time_ingest_adbc_stage_on_conflict_update(
        self, vector_dim: int, row_count: int
    ) -> None:
        """Benchmark ADBC Stage mode ingest of vector data with ON_CONFLICT='update'."""
        table_name = f"{self.table_base}_stage_update"
        with self._adbc_conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.adbc_statement.set_options(
                **{
                    adbc_driver_hologres.StatementOptions.INGEST_MODE.value: "stage",
                    adbc_driver_hologres.StatementOptions.ON_CONFLICT.value: "update",
                }
            )
            cursor.adbc_ingest(
                table_name, self.test_data, mode="create_append"
            )

    # asyncpg/psycopg2 executemany are row-by-row INSERT (~800 rows/s on
    # Hologres), and each row carries a large vector payload.
    # 1M+ rows is infeasible; even 100K is very slow.
    _small_row_params = [[512, 1024], [10_000, 100_000]]

    def time_ingest_asyncpg(
        self, vector_dim: int, row_count: int
    ) -> None:
        """Benchmark asyncpg executemany INSERT of vector data."""
        if self._asyncpg_data is None:
            raise NotImplementedError(
                "asyncpg not benchmarked at large row counts"
            )

        table_name = f"{self.table_base}_asyncpg"

        async def _ingest():
            conn = await asyncpg.connect(dsn=self.uri)
            try:
                await conn.execute(f"TRUNCATE TABLE {table_name}")
                await conn.executemany(
                    f"INSERT INTO {table_name} (id, embedding) "
                    f"VALUES ($1, $2)",
                    self._asyncpg_data,
                )
            finally:
                await conn.close()

        asyncio.run(_ingest())

    time_ingest_asyncpg.params = _small_row_params

    def time_ingest_psycopg2(
        self, vector_dim: int, row_count: int
    ) -> None:
        """Benchmark psycopg2 executemany INSERT of vector data."""
        if self._psycopg2_data is None:
            raise NotImplementedError(
                "psycopg2 not benchmarked at large row counts"
            )

        table_name = f"{self.table_base}_psycopg2"
        uri = self.uri.replace("postgres://", "postgresql+psycopg2://")
        engine = sqlalchemy.create_engine(uri)
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text(f"TRUNCATE TABLE {table_name}"))
            conn.commit()
            stmt = sqlalchemy.text(
                f"INSERT INTO {table_name} (id, embedding) "
                f"VALUES (:id, :embedding)"
            )
            conn.execute(stmt, self._psycopg2_data)
            conn.commit()

    time_ingest_psycopg2.params = _small_row_params
