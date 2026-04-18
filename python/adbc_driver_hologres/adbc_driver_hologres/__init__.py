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

import enum
import functools
import typing

import adbc_driver_manager

from ._version import __version__

__all__ = [
    "HologresIngestMode",
    "HologresOnConflict",
    "StatementOptions",
    "connect",
    "__version__",
]


class HologresOnConflict(enum.Enum):
    """ON_CONFLICT behavior for Hologres bulk ingestion."""

    #: Default: error on conflict.
    NONE = "none"
    #: Skip conflicting rows.
    IGNORE = "ignore"
    #: Update conflicting rows.
    UPDATE = "update"


class HologresIngestMode(enum.Enum):
    """Ingestion method for Hologres bulk data loading."""

    #: Standard PostgreSQL COPY protocol (default).
    COPY = "copy"
    #: Hologres internal Stage-based ingestion for larger datasets.
    STAGE = "stage"


class StatementOptions(enum.Enum):
    """Statement options specific to the Hologres driver."""

    #: Try to limit returned batches to this size (in bytes).
    BATCH_SIZE_HINT_BYTES = "adbc.hologres.batch_size_hint_bytes"

    #: Enable or disable the ``COPY`` optimization (default: enabled).
    USE_COPY = "adbc.hologres.use_copy"

    #: ON_CONFLICT mode for bulk ingestion.
    ON_CONFLICT = "adbc.hologres.on_conflict"

    #: Ingestion method: ``copy`` or ``stage``.
    INGEST_MODE = "adbc.hologres.ingest_mode"


def connect(
    uri: str,
    db_kwargs: typing.Optional[typing.Dict[str, str]] = None,
) -> adbc_driver_manager.AdbcDatabase:
    """Create a low level ADBC connection to Hologres."""
    db_options = dict(db_kwargs or {})
    db_options["driver"] = _driver_path()
    db_options["uri"] = uri
    return adbc_driver_manager.AdbcDatabase(**db_options)


@functools.lru_cache
def _driver_path() -> str:
    import pathlib
    import sys

    import importlib_resources

    driver = "adbc_driver_hologres"

    # Wheels bundle the shared library
    root = importlib_resources.files(driver)
    # The filename is always the same regardless of platform
    entrypoint = root.joinpath(f"lib{driver}.so")
    if entrypoint.is_file():
        return str(entrypoint)

    # Search sys.prefix + '/lib' (Unix, Conda on Unix)
    root = pathlib.Path(sys.prefix)
    for filename in (f"lib{driver}.so", f"lib{driver}.dylib"):
        entrypoint = root.joinpath("lib", filename)
        if entrypoint.is_file():
            return str(entrypoint)

    # Conda on Windows
    entrypoint = root.joinpath("bin", f"{driver}.dll")
    if entrypoint.is_file():
        return str(entrypoint)

    # Let the driver manager fall back to (DY)LD_LIBRARY_PATH/PATH
    return driver
