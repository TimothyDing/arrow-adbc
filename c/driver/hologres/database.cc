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

#include "database.h"

#include <cstring>

#include "driver/common/utils.h"

namespace adbchg {

AdbcStatusCode HologresDatabase::Init(struct AdbcError* error) {
  if (uri_.empty()) {
    InternalAdbcSetError(error, "%s",
                         "[hologres] Must set URI before initializing database");
    return ADBC_STATUS_INVALID_STATE;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresDatabase::Release(struct AdbcError* error) {
  uri_.clear();
  return ADBC_STATUS_OK;
}

AdbcStatusCode HologresDatabase::SetOption(const char* key, const char* value,
                                           struct AdbcError* error) {
  if (std::strcmp(key, ADBC_OPTION_URI) == 0) {
    uri_ = value;
    return ADBC_STATUS_OK;
  }
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresDatabase::GetOption(const char* key, char* value, size_t* length,
                                           struct AdbcError* error) {
  if (std::strcmp(key, ADBC_OPTION_URI) == 0) {
    size_t required = uri_.size() + 1;
    if (*length >= required) {
      std::memcpy(value, uri_.c_str(), required);
    }
    *length = required;
    return ADBC_STATUS_OK;
  }
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresDatabase::SetOptionBytes(const char* key, const uint8_t* value,
                                                size_t length, struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresDatabase::GetOptionBytes(const char* key, uint8_t* value,
                                                size_t* length, struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresDatabase::SetOptionDouble(const char* key, double value,
                                                 struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresDatabase::GetOptionDouble(const char* key, double* value,
                                                 struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode HologresDatabase::SetOptionInt(const char* key, int64_t value,
                                              struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode HologresDatabase::GetOptionInt(const char* key, int64_t* value,
                                              struct AdbcError* error) {
  InternalAdbcSetError(error, "[hologres] Unknown database option: %s", key);
  return ADBC_STATUS_NOT_FOUND;
}

}  // namespace adbchg
