// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <string>

#include <arrow-adbc/adbc.h>

namespace adbchg {

/// Hologres database (connection factory and configuration).
class HologresDatabase {
 public:
  HologresDatabase() = default;
  ~HologresDatabase() = default;

  // Lifecycle
  AdbcStatusCode Init(struct AdbcError* error);
  AdbcStatusCode Release(struct AdbcError* error);

  // Options
  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error);
  AdbcStatusCode GetOption(const char* key, char* value, size_t* length,
                           struct AdbcError* error);
  AdbcStatusCode SetOptionBytes(const char* key, const uint8_t* value, size_t length,
                                struct AdbcError* error);
  AdbcStatusCode GetOptionBytes(const char* key, uint8_t* value, size_t* length,
                                struct AdbcError* error);
  AdbcStatusCode SetOptionDouble(const char* key, double value, struct AdbcError* error);
  AdbcStatusCode GetOptionDouble(const char* key, double* value, struct AdbcError* error);
  AdbcStatusCode SetOptionInt(const char* key, int64_t value, struct AdbcError* error);
  AdbcStatusCode GetOptionInt(const char* key, int64_t* value, struct AdbcError* error);

  /// Get the connection URI.
  const std::string& uri() const { return uri_; }

 private:
  std::string uri_;
};

}  // namespace adbchg
