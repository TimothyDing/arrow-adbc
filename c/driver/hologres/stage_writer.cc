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

#include "stage_writer.h"

#include <cstdlib>
#include <random>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include <fmt/core.h>

namespace adbchg {

namespace {

/// Check if the schema contains any FIXED_SIZE_LIST fields.
static bool FindFixedSizeListFields(
    const struct ArrowSchema* schema,
    std::vector<std::pair<int, int32_t>>* fsl_fields) {
  fsl_fields->clear();
  for (int i = 0; i < schema->n_children; i++) {
    struct ArrowSchemaView view;
    struct ArrowError error;
    if (ArrowSchemaViewInit(&view, schema->children[i], &error) == NANOARROW_OK) {
      if (view.type == NANOARROW_TYPE_FIXED_SIZE_LIST) {
        fsl_fields->emplace_back(i, view.fixed_size);
      }
    }
  }
  return !fsl_fields->empty();
}

/// Deep-copy schema and convert FIXED_SIZE_LIST → LIST.
/// Hologres EXTERNAL_FILES supports LIST but not FIXED_SIZE_LIST.
static ArrowErrorCode ConvertFixedSizeListSchema(const struct ArrowSchema* src,
                                                 struct ArrowSchema* dst,
                                                 struct ArrowError* error) {
  NANOARROW_RETURN_NOT_OK(ArrowSchemaDeepCopy(src, dst));

  for (int i = 0; i < dst->n_children; i++) {
    struct ArrowSchemaView view;
    NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&view, dst->children[i], error));
    if (view.type == NANOARROW_TYPE_FIXED_SIZE_LIST) {
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetFormat(dst->children[i], "+l"));
    }
  }
  return NANOARROW_OK;
}

/// Convert FIXED_SIZE_LIST arrays to LIST by inserting offsets buffers.
static ArrowErrorCode ConvertFixedSizeListArrays(
    struct ArrowArray* array,
    const std::vector<std::pair<int, int32_t>>& fsl_fields,
    struct ArrowError* error) {
  for (const auto& [col_index, list_size] : fsl_fields) {
    struct ArrowArray* list_arr = array->children[col_index];

    int64_t n_offsets = list_arr->length + 1;

    if (list_arr->n_buffers != 1) {
      ArrowErrorSet(error,
                    "Expected 1 buffer for FIXED_SIZE_LIST parent at column %d, got %d",
                    col_index, list_arr->n_buffers);
      return EINVAL;
    }

    const void* validity_buffer = list_arr->buffers[0];

    auto** new_buffers =
        static_cast<const void**>(std::malloc(2 * sizeof(void*)));
    if (new_buffers == nullptr) {
      return ENOMEM;
    }

    auto* offsets_buf =
        static_cast<int32_t*>(ArrowRealloc(nullptr, n_offsets * sizeof(int32_t)));
    if (offsets_buf == nullptr) {
      std::free(new_buffers);
      return ENOMEM;
    }
    for (int64_t j = 0; j < n_offsets; j++) {
      offsets_buf[j] = static_cast<int32_t>(j * list_size);
    }

    new_buffers[0] = validity_buffer;
    new_buffers[1] = offsets_buf;
    list_arr->buffers = new_buffers;
    list_arr->n_buffers = 2;
    list_arr->offset = 0;

    if (list_arr->n_children > 0) {
      list_arr->children[0]->offset = 0;
      list_arr->children[0]->length = list_arr->length * list_size;
    }
  }
  return NANOARROW_OK;
}

/// Estimate the serialized size of an ArrowArray in bytes.
static int64_t EstimateArraySize(
    const struct ArrowSchema* schema, const struct ArrowArray* array,
    const std::vector<std::pair<int, int32_t>>& fsl_fields) {
  if (array->length == 0) return 0;

  int64_t row_size = 0;
  for (int i = 0; i < schema->n_children; i++) {
    struct ArrowSchemaView view;
    struct ArrowError error;
    if (ArrowSchemaViewInit(&view, schema->children[i], &error) != NANOARROW_OK) {
      continue;
    }
    switch (view.type) {
      case NANOARROW_TYPE_BOOL:
        row_size += 1;
        break;
      case NANOARROW_TYPE_INT8:
      case NANOARROW_TYPE_UINT8:
        row_size += 1;
        break;
      case NANOARROW_TYPE_INT16:
      case NANOARROW_TYPE_UINT16:
        row_size += 2;
        break;
      case NANOARROW_TYPE_INT32:
      case NANOARROW_TYPE_UINT32:
      case NANOARROW_TYPE_FLOAT:
        row_size += 4;
        break;
      case NANOARROW_TYPE_INT64:
      case NANOARROW_TYPE_UINT64:
      case NANOARROW_TYPE_DOUBLE:
        row_size += 8;
        break;
      case NANOARROW_TYPE_STRING:
      case NANOARROW_TYPE_LARGE_STRING:
        row_size += 64;
        break;
      case NANOARROW_TYPE_BINARY:
      case NANOARROW_TYPE_LARGE_BINARY:
        row_size += 32;
        break;
      case NANOARROW_TYPE_LIST: {
        int64_t avg_list_len = 10;
        for (const auto& [idx, sz] : fsl_fields) {
          if (idx == i) {
            avg_list_len = sz;
            break;
          }
        }
        int64_t child_size = 8;
        if (schema->children[i]->n_children > 0) {
          struct ArrowSchemaView child_view;
          if (ArrowSchemaViewInit(&child_view, schema->children[i]->children[0],
                                 &error) == NANOARROW_OK) {
            switch (child_view.type) {
              case NANOARROW_TYPE_FLOAT:
                child_size = 4;
                break;
              case NANOARROW_TYPE_DOUBLE:
                child_size = 8;
                break;
              case NANOARROW_TYPE_INT32:
                child_size = 4;
                break;
              case NANOARROW_TYPE_INT64:
                child_size = 8;
                break;
              default:
                break;
            }
          }
        }
        row_size += child_size * avg_list_len;
        break;
      }
      case NANOARROW_TYPE_TIMESTAMP:
        row_size += 8;
        break;
      case NANOARROW_TYPE_DATE32:
        row_size += 4;
        break;
      default:
        row_size += 8;
        break;
    }
  }
  return array->length * row_size;
}

}  // namespace

// BufferQueue implementation
void BufferQueue::Push(std::unique_ptr<StageBuffer> buffer) {
  std::lock_guard<std::mutex> lock(mutex_);
  queue_.push(std::move(buffer));
  cv_.notify_one();
}

std::unique_ptr<StageBuffer> BufferQueue::Pop() {
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait(lock, [this] { return closed_ || !queue_.empty(); });

  if (queue_.empty()) {
    return nullptr;
  }

  auto buffer = std::move(queue_.front());
  queue_.pop();
  return buffer;
}

void BufferQueue::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  closed_ = true;
  cv_.notify_all();
}

bool BufferQueue::IsClosed() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return closed_;
}

// HologresStageWriter implementation
HologresStageWriter::HologresStageWriter(StageConnection* conn,
                                         const HologresStageConfig& config)
    : conn_(conn), config_(config) {}

HologresStageWriter::~HologresStageWriter() {
  for (auto& thread : upload_threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

std::string HologresStageWriter::GenerateStageName() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 15);
  std::uniform_int_distribution<> dis2(8, 11);

  std::stringstream ss;
  ss << "adbc_hg_stage_";

  for (int i = 0; i < 8; i++) ss << std::hex << dis(gen);
  ss << "-";
  for (int i = 0; i < 4; i++) ss << std::hex << dis(gen);
  ss << "-4";
  for (int i = 0; i < 3; i++) ss << std::hex << dis(gen);
  ss << "-";
  ss << std::hex << dis2(gen);
  for (int i = 0; i < 3; i++) ss << std::hex << dis(gen);
  ss << "-";
  for (int i = 0; i < 12; i++) ss << std::hex << dis(gen);

  return ss.str();
}

std::string HologresStageWriter::FormatFileName(int index) const {
  return fmt::format("data_{}.arrow", index);
}

Status HologresStageWriter::CreateStage() {
  std::string sql = fmt::format(
      "CALL HOLOGRES.HG_CREATE_INTERNAL_STAGE('{}', '{}', {})",
      config_.stage_name, config_.group_name, config_.ttl_seconds);

  Status status = conn_->ExecuteCommand(sql);
  if (!status.ok()) {
    return status;
  }

  stage_created_ = true;
  return Status::Ok();
}

Status HologresStageWriter::DropStage() {
  if (!stage_created_) {
    return Status::Ok();
  }

  std::string sql = fmt::format("CALL HOLOGRES.HG_DROP_INTERNAL_STAGE('{}')",
                                config_.stage_name);

  return conn_->ExecuteCommand(sql);
}

void HologresStageWriter::UploadThread() {
  while (true) {
    auto buffer = buffer_queue_->Pop();
    if (!buffer) {
      break;
    }

    Status status = UploadBufferToStage(*buffer);
    if (!status.ok()) {
      std::lock_guard<std::mutex> lock(error_mutex_);
      if (!error_flag_) {
        error_flag_ = true;
        AdbcError adbc_error{};
        status.ToAdbc(&adbc_error);
        error_message_ =
            fmt::format("Upload failed for file {}: {}", buffer->file_name,
                        adbc_error.message ? adbc_error.message : "unknown error");
        if (adbc_error.release) {
          adbc_error.release(&adbc_error);
        }
      }
    }
  }
}

Status HologresStageWriter::UploadBufferToStage(const StageBuffer& buffer) {
  std::string sql = fmt::format(
      "COPY EXTERNAL_FILES(path='internal_stage://{}/{}') FROM STDIN",
      config_.stage_name, buffer.file_name);

  std::string error_msg;
  return conn_->ExecuteCopy(sql, reinterpret_cast<const char*>(buffer.data.data()),
                            static_cast<int64_t>(buffer.data.size()), &error_msg);
}

std::string HologresStageWriter::BuildOnConflictClause(
    OnConflictMode mode, const std::string& pk_columns) const {
  if (mode == OnConflictMode::kNone || pk_columns.empty()) {
    return "";
  }

  return fmt::format(" ON CONFLICT ({}) DO {}",
                     pk_columns,
                     mode == OnConflictMode::kIgnore ? "NOTHING" : "UPDATE SET ");
}

Status HologresStageWriter::SerializeArrayToIpcBuffer(
    const struct ArrowSchema* schema, const struct ArrowArray* array,
    std::vector<uint8_t>* out_buffer, struct ArrowError* error) {
  struct ArrowBuffer ipc_buffer;
  ArrowBufferInit(&ipc_buffer);

  struct ArrowIpcOutputStream output_stream;
  int result = ArrowIpcOutputStreamInitBuffer(&output_stream, &ipc_buffer);
  if (result != NANOARROW_OK) {
    ArrowBufferReset(&ipc_buffer);
    return Status::IO("[hologres] Failed to init IPC output stream");
  }

  struct ArrowIpcWriter writer;
  result = ArrowIpcWriterInit(&writer, &output_stream);
  if (result != NANOARROW_OK) {
    ArrowBufferReset(&ipc_buffer);
    return Status::IO("[hologres] Failed to init IPC writer");
  }

  result = ArrowIpcWriterWriteSchema(&writer, schema, error);
  if (result != NANOARROW_OK) {
    ArrowIpcWriterReset(&writer);
    ArrowBufferReset(&ipc_buffer);
    return Status::IO("[hologres] Failed to write schema: ", error->message);
  }

  struct ArrowArrayView array_view;
  ArrowArrayViewInitFromType(&array_view, NANOARROW_TYPE_UNINITIALIZED);
  result = ArrowArrayViewInitFromSchema(&array_view, schema, error);
  if (result != NANOARROW_OK) {
    ArrowIpcWriterReset(&writer);
    ArrowBufferReset(&ipc_buffer);
    return Status::IO("[hologres] Failed to init array view: ", error->message);
  }

  result = ArrowArrayViewSetArray(&array_view, array, error);
  if (result != NANOARROW_OK) {
    ArrowArrayViewReset(&array_view);
    ArrowIpcWriterReset(&writer);
    ArrowBufferReset(&ipc_buffer);
    return Status::IO("[hologres] Failed to set array view: ", error->message);
  }

  result = ArrowIpcWriterWriteArrayView(&writer, &array_view, error);
  if (result != NANOARROW_OK) {
    ArrowArrayViewReset(&array_view);
    ArrowIpcWriterReset(&writer);
    ArrowBufferReset(&ipc_buffer);
    return Status::IO("[hologres] Failed to write array: ", error->message);
  }

  result = ArrowIpcWriterWriteArrayView(&writer, nullptr, error);
  if (result != NANOARROW_OK) {
    ArrowArrayViewReset(&array_view);
    ArrowIpcWriterReset(&writer);
    ArrowBufferReset(&ipc_buffer);
    return Status::IO("[hologres] Failed to write EOS marker: ", error->message);
  }

  out_buffer->assign(ipc_buffer.data, ipc_buffer.data + ipc_buffer.size_bytes);

  ArrowArrayViewReset(&array_view);
  ArrowIpcWriterReset(&writer);
  ArrowBufferReset(&ipc_buffer);

  return Status::Ok();
}

Status HologresStageWriter::WriteToStage(struct ArrowArrayStream* stream,
                                         int64_t* rows_affected) {
  if (rows_affected) *rows_affected = 0;

  buffer_queue_ = std::make_unique<BufferQueue>();

  for (int i = 0; i < config_.upload_concurrency; i++) {
    upload_threads_.emplace_back(&HologresStageWriter::UploadThread, this);
  }

  nanoarrow::UniqueSchema schema;
  struct ArrowError na_error;
  ArrowErrorInit(&na_error);

  int result = ArrowArrayStreamGetSchema(stream, schema.get(), &na_error);
  if (result != NANOARROW_OK) {
    buffer_queue_->Close();
    for (auto& thread : upload_threads_) {
      if (thread.joinable()) thread.join();
    }
    return Status::IO("[hologres] Failed to get schema from stream: ",
                      na_error.message);
  }

  std::vector<std::pair<int, int32_t>> fsl_fields;
  nanoarrow::UniqueSchema ipc_schema;
  bool needs_fsl_conversion = FindFixedSizeListFields(schema.get(), &fsl_fields);

  if (needs_fsl_conversion) {
    result = ConvertFixedSizeListSchema(schema.get(), ipc_schema.get(), &na_error);
    if (result != NANOARROW_OK) {
      buffer_queue_->Close();
      for (auto& thread : upload_threads_) {
        if (thread.joinable()) thread.join();
      }
      return Status::IO(
          "[hologres] Failed to convert FIXED_SIZE_LIST schema for stage: ",
          na_error.message);
    }
  }

  struct ArrowSchema* ipc_schema_ptr =
      needs_fsl_conversion ? ipc_schema.get() : schema.get();

  nanoarrow::UniqueArray array;

  while (true) {
    result = ArrowArrayStreamGetNext(stream, array.get(), &na_error);
    if (result != NANOARROW_OK) {
      buffer_queue_->Close();
      for (auto& thread : upload_threads_) {
        if (thread.joinable()) thread.join();
      }
      return Status::IO("[hologres] Failed to get array from stream: ",
                        na_error.message);
    }

    if (array->release == nullptr) {
      break;
    }

    if (needs_fsl_conversion) {
      result = ConvertFixedSizeListArrays(array.get(), fsl_fields, &na_error);
      if (result != NANOARROW_OK) {
        buffer_queue_->Close();
        for (auto& thread : upload_threads_) {
          if (thread.joinable()) thread.join();
        }
        return Status::IO(
            "[hologres] Failed to convert FIXED_SIZE_LIST arrays for stage: ",
            na_error.message);
      }
    }

    int64_t estimated_size =
        EstimateArraySize(ipc_schema_ptr, array.get(), fsl_fields);
    int64_t rows_per_slice = array->length;

    if (estimated_size > config_.target_file_size && array->length > 1) {
      int64_t bytes_per_row = estimated_size / array->length;
      if (bytes_per_row > 0) {
        rows_per_slice = config_.target_file_size / bytes_per_row;
        if (rows_per_slice < 1) rows_per_slice = 1;
      }
    }

    // Precompute child element byte sizes for FSL columns
    std::unordered_map<int, int64_t> fsl_child_byte_sizes;
    for (const auto& [col_idx, list_size] : fsl_fields) {
      struct ArrowSchemaView child_view;
      struct ArrowError child_error;
      if (ipc_schema_ptr->children[col_idx]->n_children > 0 &&
          ArrowSchemaViewInit(&child_view,
                              ipc_schema_ptr->children[col_idx]->children[0],
                              &child_error) == NANOARROW_OK) {
        fsl_child_byte_sizes[col_idx] =
            child_view.layout.element_size_bits[1] / 8;
      } else {
        fsl_child_byte_sizes[col_idx] = 0;
      }
    }

    std::unordered_set<int> fsl_col_indices;
    for (const auto& [idx, _] : fsl_fields) {
      fsl_col_indices.insert(idx);
    }

    // Precompute element byte sizes for non-FSL fixed-width columns
    std::unordered_map<int, int64_t> non_fsl_child_byte_sizes;
    for (int i = 0; i < ipc_schema_ptr->n_children; i++) {
      if (fsl_col_indices.count(i)) continue;
      struct ArrowSchemaView view;
      struct ArrowError err;
      if (ArrowSchemaViewInit(&view, ipc_schema_ptr->children[i], &err) ==
          NANOARROW_OK) {
        int64_t byte_size = view.layout.element_size_bits[1] / 8;
        if (byte_size > 0) {
          non_fsl_child_byte_sizes[i] = byte_size;
        }
      }
    }

    // Process the batch, potentially splitting into multiple slices
    int64_t rows_processed = 0;
    while (rows_processed < array->length) {
      int64_t slice_length =
          std::min(rows_per_slice, array->length - rows_processed);

      int64_t orig_offset = array->offset;
      int64_t orig_length = array->length;

      // Slice state for non-FSL child columns
      struct ChildSliceState {
        struct ArrowArray* child_arr;
        int64_t old_offset;
        int64_t old_length;
        const void* old_validity_buffer;
        const void* old_data_buffer;
        int64_t old_null_count;
      };
      std::vector<ChildSliceState> child_states;
      child_states.reserve(non_fsl_child_byte_sizes.size());

      for (const auto& [col_idx, byte_size] : non_fsl_child_byte_sizes) {
        struct ArrowArray* child = array->children[col_idx];
        ChildSliceState cs;
        cs.child_arr = child;
        cs.old_offset = child->offset;
        cs.old_length = child->length;
        cs.old_validity_buffer = child->buffers[0];
        cs.old_data_buffer = child->buffers[1];
        cs.old_null_count = child->null_count;

        int64_t byte_offset = rows_processed * byte_size;
        child->buffers[1] =
            static_cast<const uint8_t*>(child->buffers[1]) + byte_offset;
        child->offset = 0;
        child->length = slice_length;
        child->buffers[0] = nullptr;
        child->null_count = 0;

        child_states.push_back(cs);
      }

      // Slice state for FSL→LIST columns
      struct ListSliceState {
        struct ArrowArray* list_arr;
        const void** old_buffers;
        int64_t old_n_buffers;
        int64_t old_length;
        int64_t old_offset;
        int64_t old_null_count;
        int64_t old_child_offset;
        int64_t old_child_length;
        const void* old_child_data_buffer;
        const void** new_buffers;
        int32_t* new_offsets;
      };
      std::vector<ListSliceState> slice_states;
      slice_states.reserve(fsl_fields.size());

      bool alloc_failed = false;
      for (const auto& [col_idx, list_size] : fsl_fields) {
        struct ArrowArray* list_arr = array->children[col_idx];
        ListSliceState state;
        state.list_arr = list_arr;
        state.old_buffers = list_arr->buffers;
        state.old_n_buffers = list_arr->n_buffers;
        state.old_length = list_arr->length;
        state.old_offset = list_arr->offset;
        state.old_null_count = list_arr->null_count;

        int64_t child_start = static_cast<int64_t>(rows_processed) * list_size;
        int64_t child_end =
            static_cast<int64_t>(rows_processed + slice_length) * list_size;

        int64_t n_slice_offsets = slice_length + 1;
        auto* slice_offsets = static_cast<int32_t*>(
            std::malloc(n_slice_offsets * sizeof(int32_t)));
        if (slice_offsets == nullptr) {
          alloc_failed = true;
          break;
        }
        for (int64_t j = 0; j < n_slice_offsets; j++) {
          slice_offsets[j] = static_cast<int32_t>(j * list_size);
        }

        auto** slice_buffers =
            static_cast<const void**>(std::malloc(2 * sizeof(void*)));
        if (slice_buffers == nullptr) {
          std::free(slice_offsets);
          alloc_failed = true;
          break;
        }
        slice_buffers[0] = nullptr;
        slice_buffers[1] = slice_offsets;

        if (list_arr->n_children > 0) {
          state.old_child_offset = list_arr->children[0]->offset;
          state.old_child_length = list_arr->children[0]->length;
          state.old_child_data_buffer = list_arr->children[0]->buffers[1];

          auto it = fsl_child_byte_sizes.find(col_idx);
          if (it != fsl_child_byte_sizes.end() && it->second > 0) {
            int64_t byte_offset = child_start * it->second;
            list_arr->children[0]->buffers[1] =
                static_cast<const uint8_t*>(
                    list_arr->children[0]->buffers[1]) +
                byte_offset;
          }
          list_arr->children[0]->offset = 0;
          list_arr->children[0]->length = child_end - child_start;
        }

        state.new_buffers = slice_buffers;
        state.new_offsets = slice_offsets;

        list_arr->buffers = slice_buffers;
        list_arr->n_buffers = 2;
        list_arr->length = slice_length;
        list_arr->offset = 0;
        list_arr->null_count = 0;

        slice_states.push_back(state);
      }

      if (alloc_failed) {
        for (auto& st : slice_states) {
          st.list_arr->buffers = st.old_buffers;
          st.list_arr->n_buffers = st.old_n_buffers;
          st.list_arr->length = st.old_length;
          st.list_arr->offset = st.old_offset;
          st.list_arr->null_count = st.old_null_count;
          if (st.list_arr->n_children > 0) {
            st.list_arr->children[0]->offset = st.old_child_offset;
            st.list_arr->children[0]->length = st.old_child_length;
            st.list_arr->children[0]->buffers[1] = st.old_child_data_buffer;
          }
          std::free(st.new_offsets);
          std::free(st.new_buffers);
        }
        for (auto& cs : child_states) {
          cs.child_arr->offset = cs.old_offset;
          cs.child_arr->length = cs.old_length;
          cs.child_arr->buffers[0] = cs.old_validity_buffer;
          cs.child_arr->buffers[1] = cs.old_data_buffer;
          cs.child_arr->null_count = cs.old_null_count;
        }
        array->offset = orig_offset;
        array->length = orig_length;
        buffer_queue_->Close();
        for (auto& thread : upload_threads_) {
          if (thread.joinable()) thread.join();
        }
        return Status::IO("[hologres] Failed to allocate slice buffers");
      }

      array->offset = 0;
      array->length = slice_length;

      std::vector<uint8_t> ipc_data;
      Status ipc_status = SerializeArrayToIpcBuffer(
          ipc_schema_ptr, array.get(), &ipc_data, &na_error);

      // Restore original state
      array->offset = orig_offset;
      array->length = orig_length;

      for (auto& st : slice_states) {
        st.list_arr->buffers = st.old_buffers;
        st.list_arr->n_buffers = st.old_n_buffers;
        st.list_arr->length = st.old_length;
        st.list_arr->offset = st.old_offset;
        st.list_arr->null_count = st.old_null_count;
        if (st.list_arr->n_children > 0) {
          st.list_arr->children[0]->offset = st.old_child_offset;
          st.list_arr->children[0]->length = st.old_child_length;
          st.list_arr->children[0]->buffers[1] = st.old_child_data_buffer;
        }
        std::free(st.new_offsets);
        std::free(st.new_buffers);
      }

      for (auto& cs : child_states) {
        cs.child_arr->offset = cs.old_offset;
        cs.child_arr->length = cs.old_length;
        cs.child_arr->buffers[0] = cs.old_validity_buffer;
        cs.child_arr->buffers[1] = cs.old_data_buffer;
        cs.child_arr->null_count = cs.old_null_count;
      }

      if (!ipc_status.ok()) {
        buffer_queue_->Close();
        for (auto& thread : upload_threads_) {
          if (thread.joinable()) thread.join();
        }
        return ipc_status;
      }

      total_rows_ += slice_length;

      auto buffer = std::make_unique<StageBuffer>();
      buffer->data = std::move(ipc_data);
      buffer->file_name = FormatFileName(file_index_++);
      buffer->rows = slice_length;

      buffer_queue_->Push(std::move(buffer));

      rows_processed += slice_length;
    }

    array.reset();
  }

  buffer_queue_->Close();
  for (auto& thread : upload_threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  if (error_flag_) {
    return Status::IO(error_message_);
  }

  if (rows_affected) *rows_affected = total_rows_;
  return Status::Ok();
}

Status HologresStageWriter::InsertFromStage(const std::string& target_table,
                                            const std::string& escaped_field_list,
                                            const std::string& escaped_type_list,
                                            OnConflictMode on_conflict) {
  std::string sql = fmt::format(
      "INSERT INTO {} ({}) SELECT * FROM EXTERNAL_FILES("
      "path='internal_stage://{}') AS ({})",
      target_table, escaped_field_list, config_.stage_name, escaped_type_list);

  if (on_conflict != OnConflictMode::kNone) {
    if (on_conflict == OnConflictMode::kIgnore) {
      sql += " ON CONFLICT DO NOTHING";
    }
  }

  return conn_->ExecuteCommand(sql);
}

}  // namespace adbchg
