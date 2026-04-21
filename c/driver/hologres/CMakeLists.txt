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
  pkg_check_modules(LZ4 REQUIRED liblz4)
endif()

add_arrow_lib(adbc_driver_hologres
              SOURCES
              arrow_copy_reader.cc
              connection.cc
              database.cc
              error.cc
              hologres.cc
              result_helper.cc
              result_reader.cc
              stage_connection.cc
              stage_writer.cc
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
              ${LZ4_LINK_LIBRARIES}
              STATIC_LINK_LIBS
              ${LIBPQ_LINK_LIBRARIES}
              ${LZ4_LINK_LIBRARIES}
              adbc_driver_common
              adbc_driver_framework
              ${LIBPQ_STATIC_LIBRARIES})

foreach(LIB_TARGET ${ADBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING
                            ADBC_HOLOGRES_VERSION="${ADBC_BASE_VERSION}")
  target_include_directories(${LIB_TARGET} SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${LZ4_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${REPOSITORY_ROOT}/c/vendor/portable-snippets/include
  )

  if(NOT ADBC_DEFINE_COMMON_ENTRYPOINTS)
    target_compile_definitions(${LIB_TARGET} PRIVATE ${ADBC_TARGET_COMPILE_DEFINITIONS})
  endif()
endforeach()

if(ADBC_TEST_LINKAGE STREQUAL "shared")
  set(TEST_LINK_LIBS adbc_driver_hologres_shared)
else()
  set(TEST_LINK_LIBS adbc_driver_hologres_static)
endif()

if(ADBC_BUILD_TESTS)
  add_test_case(driver_hologres_stage_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-hologres
                SOURCES
                stage_writer_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-hologres-stage-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-hologres-stage-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${REPOSITORY_ROOT}/c/vendor/portable-snippets/include
  )
  adbc_configure_target(adbc-driver-hologres-stage-test)

  add_test_case(driver_hologres_database_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-hologres
                SOURCES
                database_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-hologres-database-test PRIVATE cxx_std_17)
  target_compile_definitions(adbc-driver-hologres-database-test PRIVATE
                             ADBC_HOLOGRES_VERSION="${ADBC_BASE_VERSION}")
  target_include_directories(adbc-driver-hologres-database-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${REPOSITORY_ROOT}/c/vendor/portable-snippets/include
  )
  adbc_configure_target(adbc-driver-hologres-database-test)

  add_test_case(driver_hologres_connection_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-hologres
                SOURCES
                connection_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-hologres-connection-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-hologres-connection-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${REPOSITORY_ROOT}/c/vendor/portable-snippets/include
  )
  adbc_configure_target(adbc-driver-hologres-connection-test)

  add_test_case(driver_hologres_statement_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-hologres
                SOURCES
                statement_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-hologres-statement-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-hologres-statement-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${REPOSITORY_ROOT}/c/vendor/portable-snippets/include
  )
  adbc_configure_target(adbc-driver-hologres-statement-test)

  add_test_case(driver_hologres_postgres_util_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-hologres
                SOURCES
                postgres_util_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-hologres-postgres-util-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-hologres-postgres-util-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${REPOSITORY_ROOT}/c/vendor/portable-snippets/include
  )
  adbc_configure_target(adbc-driver-hologres-postgres-util-test)

  add_test_case(driver_hologres_postgres_type_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-hologres
                SOURCES
                postgres_type_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-hologres-postgres-type-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-hologres-postgres-type-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${REPOSITORY_ROOT}/c/vendor/portable-snippets/include
  )
  adbc_configure_target(adbc-driver-hologres-postgres-type-test)

  add_test_case(driver_hologres_copy_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-hologres
                SOURCES
                copy_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-hologres-copy-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-hologres-copy-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${REPOSITORY_ROOT}/c/vendor/portable-snippets/include
  )
  adbc_configure_target(adbc-driver-hologres-copy-test)

  add_test_case(driver_hologres_copy_writer_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-hologres
                SOURCES
                copy_writer_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-hologres-copy-writer-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-hologres-copy-writer-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${REPOSITORY_ROOT}/c/vendor/portable-snippets/include
  )
  adbc_configure_target(adbc-driver-hologres-copy-writer-test)

  add_test_case(driver_hologres_error_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-hologres
                SOURCES
                error_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-hologres-error-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-hologres-error-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${REPOSITORY_ROOT}/c/vendor/portable-snippets/include
  )
  adbc_configure_target(adbc-driver-hologres-error-test)

  add_test_case(driver_hologres_arrow_copy_reader_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-hologres
                SOURCES
                arrow_copy_reader_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                ${TEST_LINK_LIBS}
                ${LZ4_LINK_LIBRARIES})
  target_compile_features(adbc-driver-hologres-arrow-copy-reader-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-hologres-arrow-copy-reader-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${LZ4_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${REPOSITORY_ROOT}/c/vendor/portable-snippets/include
  )
  adbc_configure_target(adbc-driver-hologres-arrow-copy-reader-test)

  add_test_case(driver_hologres_bind_stream_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-hologres
                SOURCES
                bind_stream_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-hologres-bind-stream-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-hologres-bind-stream-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${REPOSITORY_ROOT}/c/vendor/portable-snippets/include
  )
  adbc_configure_target(adbc-driver-hologres-bind-stream-test)
endif()
