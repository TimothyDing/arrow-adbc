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

if(WIN32)
  # XXX: for now, assume vcpkg
  find_package(PostgreSQL REQUIRED)
  set(LIBPQ_LINK_LIBRARIES PostgreSQL::PostgreSQL wsock32 ws2_32)
  set(LIBPQ_STATIC_LIBRARIES PostgreSQL::PostgreSQL)
  set(LIBPQ_INCLUDE_DIRS)
else()
  find_package(PkgConfig)
  pkg_check_modules(LIBPQ REQUIRED libpq)
endif()

add_arrow_lib(adbc_driver_hologres
              SOURCES
              connection.cc
              database.cc
              hologres.cc
              statement.cc
              OUTPUTS
              ADBC_LIBRARIES
              CMAKE_PACKAGE_NAME
              AdbcDriverHologres
              PKG_CONFIG_NAME
              adbc-driver-hologres
              SHARED_LINK_FLAGS
              ${ADBC_LINK_FLAGS}
              SHARED_LINK_LIBS
              adbc_driver_common
              adbc_driver_framework
              ${LIBPQ_LINK_LIBRARIES}
              STATIC_LINK_LIBS
              ${LIBPQ_LINK_LIBRARIES}
              adbc_driver_common
              adbc_driver_framework
              ${LIBPQ_STATIC_LIBRARIES})

foreach(LIB_TARGET ${ADBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING)
  target_include_directories(${LIB_TARGET} SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${REPOSITORY_ROOT}/c/vendor/portable-snippets/include
  )

  if(NOT ADBC_DEFINE_COMMON_ENTRYPOINTS)
    target_compile_definitions(${LIB_TARGET} PRIVATE ${ADBC_TARGET_COMPILE_DEFINITIONS})
  endif()
endforeach()
