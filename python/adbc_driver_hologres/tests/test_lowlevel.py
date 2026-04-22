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

"""Low-level ADBC API tests for the Hologres driver."""

import typing

import pyarrow
import pytest

import adbc_driver_manager
import adbc_driver_hologres


@pytest.fixture
def hologres(
    hologres_uri: str,
) -> typing.Generator[adbc_driver_manager.AdbcConnection, None, None]:
    with adbc_driver_hologres.connect(hologres_uri) as db:
        with adbc_driver_manager.AdbcConnection(db) as conn:
            yield conn


def test_version() -> None:
    assert adbc_driver_hologres.__version__


def test_failed_connection() -> None:
    with pytest.raises(adbc_driver_manager.OperationalError):
        adbc_driver_hologres.connect("invalid://bad-uri")


def test_connection_get_table_schema(
    hologres: adbc_driver_manager.AdbcConnection,
) -> None:
    with pytest.raises(adbc_driver_manager.ProgrammingError):
        hologres.get_table_schema(None, None, "this_table_does_not_exist_xyz")


def test_query_trivial(
    hologres: adbc_driver_manager.AdbcConnection,
) -> None:
    with adbc_driver_manager.AdbcStatement(hologres) as stmt:
        stmt.set_sql_query("SELECT 1")
        stream, _ = stmt.execute_query()
        with pyarrow.RecordBatchReader._import_from_c(stream.address) as reader:
            table = reader.read_all()
            assert table.num_rows == 1
