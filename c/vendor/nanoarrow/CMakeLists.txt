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

# Enable IPC support for Hologres stage ingestion
set(NANOARROW_IPC ON CACHE BOOL "Enable Arrow IPC support" FORCE)

add_library(
  nanoarrow
  STATIC
  nanoarrow.c
)

# Add IPC sources if available
if(NANOARROW_IPC AND EXISTS "${CMAKE_CURRENT_LIST_DIR}/ipc/writer.c")
  target_sources(nanoarrow PRIVATE
    ipc/writer.c
    ipc/reader.c
    ipc/encoder.c
    ipc/decoder.c
    ipc/codecs.c
  )

  # Add flatcc runtime sources (required for IPC serialization)
  if(EXISTS "${CMAKE_CURRENT_LIST_DIR}/flatcc/src/runtime/builder.c")
    target_sources(nanoarrow PRIVATE
      flatcc/src/runtime/builder.c
      flatcc/src/runtime/emitter.c
      flatcc/src/runtime/refmap.c
      flatcc/src/runtime/verifier.c
    )
  endif()

  # Include directories for IPC:
  # - nanoarrow root (for nanoarrow.h, nanoarrow_ipc.h)
  # - current directory (for flatcc/ headers)
  # - ipc directory (for flatcc_generated.h)
  target_include_directories(nanoarrow PRIVATE
    "${CMAKE_CURRENT_LIST_DIR}/.."
    "${CMAKE_CURRENT_LIST_DIR}"
    "${CMAKE_CURRENT_LIST_DIR}/ipc"
  )

  # Enable LZ4 frame decompression for IPC body buffers.
  # Hologres sends IPC RecordBatch messages with LZ4_FRAME body compression.
  find_package(PkgConfig)
  pkg_check_modules(LZ4 liblz4)
  if(LZ4_FOUND)
    target_compile_definitions(nanoarrow PRIVATE NANOARROW_IPC_WITH_LZ4)
    target_include_directories(nanoarrow PRIVATE ${LZ4_INCLUDE_DIRS})
    target_link_libraries(nanoarrow PRIVATE ${LZ4_LINK_LIBRARIES})
  endif()
endif()

target_include_directories(nanoarrow PUBLIC "${CMAKE_CURRENT_LIST_DIR}/..")
set_target_properties(
  nanoarrow PROPERTIES POSITION_INDEPENDENT_CODE ON
)
add_library(nanoarrow::nanoarrow ALIAS nanoarrow)

# Create IPC alias if available
if(NANOARROW_IPC AND EXISTS "${CMAKE_CURRENT_LIST_DIR}/ipc/writer.c")
  add_library(nanoarrow::nanoarrow_ipc ALIAS nanoarrow)
endif()
