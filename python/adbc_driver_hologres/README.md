# ADBC Hologres Driver for Python

A [libpq](https://www.postgresql.org/docs/current/libpq.html)-based ADBC driver for [Hologres](https://www.alibabacloud.com/product/hologres) (Alibaba Cloud real-time data warehouse), with a [DBAPI 2.0](https://peps.python.org/pep-0249/)-compatible interface.

See the [main driver documentation](../../c/driver/hologres/README.md) for a comprehensive guide covering architecture, features, configuration options, and multi-language usage examples.

## Quick Start

```python
import adbc_driver_hologres.dbapi as dbapi

uri = "postgresql://user:pass@host:port/dbname"

with dbapi.connect(uri, autocommit=True) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM my_table LIMIT 10")
        table = cur.fetch_arrow_table()  # PyArrow Table
        df = cur.fetch_df()              # Pandas DataFrame
```

## Bulk Ingestion

```python
import pyarrow

data = pyarrow.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})

with dbapi.connect(uri, autocommit=True) as conn:
    with conn.cursor() as cur:
        cur.adbc_ingest("my_table", data, mode="append")
```

For Stage mode, ON_CONFLICT handling, and COPY format options, see the [main documentation](../../c/driver/hologres/README.md#6-usage-examples).

## Building

Prerequisites: a pre-built `libadbc_driver_hologres.so` from the C driver ([build instructions](../../c/driver/hologres/README.md#21-cc-core-driver)).

```bash
export ADBC_HOLOGRES_LIBRARY=/path/to/libadbc_driver_hologres.so
pip install -e ".[test]"
```

## Testing

Integration tests require a live Hologres instance:

```bash
export ADBC_HOLOGRES_TEST_URI="postgresql://user:pass@host:port/dbname"
pytest tests/ -vvx
```

## Benchmarks

Performance benchmarks comparing ADBC against asyncpg, psycopg2, and DuckDB:

```bash
export ADBC_HOLOGRES_TEST_URI="postgresql://user:pass@host:port/dbname"
asv run
```
