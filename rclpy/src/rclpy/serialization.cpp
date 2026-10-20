// Copyright 2021 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <pybind11/pybind11.h>

#include <rcl/error_handling.h>
#include <rcl/types.h>
#include <rcutils/allocator.h>
#include <rcutils/error_handling.h>
#include <rmw/rmw.h>
#include <rmw/serialized_message.h>
#include <rosidl_runtime_c/message_type_support_struct.h>

#include <algorithm>
#include <cstring>
#include <utility>

#include "exceptions.hpp"
#include "serialization.hpp"
#include "utils.hpp"

namespace rclpy
{
namespace
{
struct SerializedBytes
{
  py::object bytes;

  static void * allocate(size_t size, void * state)
  {
    if (size > static_cast<size_t>(PY_SSIZE_T_MAX)) {
      PyErr_NoMemory();
      return nullptr;
    }
    PyObject * buffer = PyBytes_FromStringAndSize(nullptr, size);
    if (!buffer) {
      return nullptr;
    }
    static_cast<SerializedBytes *>(state)->bytes = py::reinterpret_steal<py::object>(buffer);
    return PyBytes_AS_STRING(buffer);
  }

  static void deallocate(void *, void * state)
  {
    static_cast<SerializedBytes *>(state)->bytes = py::object();
  }

  static void * reallocate(void * pointer, size_t size, void * state)
  {
    auto & bytes = static_cast<SerializedBytes *>(state)->bytes;
    py::object previous = std::move(bytes);
    void * buffer = allocate(size, state);
    if (!buffer) {
      bytes = std::move(previous);
      return nullptr;
    }
    if (pointer) {
      memcpy(buffer, pointer,
        std::min(size, static_cast<size_t>(PyBytes_GET_SIZE(previous.ptr()))));
    }
    return buffer;
  }

  static void * zero_allocate(size_t count, size_t size, void * state)
  {
    if (size && count > static_cast<size_t>(PY_SSIZE_T_MAX) / size) {
      PyErr_NoMemory();
      return nullptr;
    }
    void * buffer = allocate(count * size, state);
    if (buffer) {
      memset(buffer, 0, count * size);
    }
    return buffer;
  }
};
}  // namespace

SerializedMessage::SerializedMessage(rcutils_allocator_t allocator)
{
  rcl_msg = rmw_get_zero_initialized_serialized_message();
  rcutils_ret_t rcutils_ret = rmw_serialized_message_init(&rcl_msg, 0u, &allocator);
  if (RCUTILS_RET_OK != rcutils_ret) {
    throw RCUtilsError("failed to initialize serialized message");
  }
}

SerializedMessage::~SerializedMessage()
{
  rcutils_ret_t ret = rmw_serialized_message_fini(&rcl_msg);
  if (RCUTILS_RET_OK != ret) {
    RCUTILS_SAFE_FWRITE_TO_STDERR(
      "[rclpy|" RCUTILS_STRINGIFY(__FILE__) ":" RCUTILS_STRINGIFY(__LINE__) "]: "
      "failed to fini rcl_serialized_msg_t in destructor:");
    RCUTILS_SAFE_FWRITE_TO_STDERR(rcutils_get_error_string().str);
    RCUTILS_SAFE_FWRITE_TO_STDERR("\n");
    rcutils_reset_error();
  }
}

py::bytes
serialize(py::object pymsg, py::object pymsg_type)
{
  // Get type support
  auto ts = static_cast<rosidl_message_type_support_t *>(
    common_get_type_support(pymsg_type));
  if (!ts) {
    throw py::error_already_set();
  }

  auto ros_msg = convert_from_py(pymsg);
  if (!ros_msg) {
    throw py::error_already_set();
  }

  // Keep the bytes private until the middleware has finished writing.
  SerializedBytes storage;
  rcutils_allocator_t allocator = {
    SerializedBytes::allocate, SerializedBytes::deallocate,
    SerializedBytes::reallocate, SerializedBytes::zero_allocate, &storage};
  PyObject * bytes;
  size_t length;
  {
    SerializedMessage serialized_msg(allocator);
    rmw_ret_t rmw_ret = rmw_serialize(ros_msg.get(), ts, &serialized_msg.rcl_msg);
    if (RMW_RET_OK != rmw_ret) {
      if (PyErr_Occurred()) {
        throw py::error_already_set();
      }
      throw RMWError("Failed to serialize ROS message");
    }
    length = serialized_msg.rcl_msg.buffer_length;
    bytes = storage.bytes.release().ptr();
  }
  if (_PyBytes_Resize(&bytes, length) < 0) {
    throw py::error_already_set();
  }
  return py::reinterpret_steal<py::bytes>(bytes);
}

py::object
deserialize(py::bytes pybuffer, py::object pymsg_type)
{
  // Get type support
  auto ts = static_cast<rosidl_message_type_support_t *>(
    common_get_type_support(pymsg_type));
  if (!ts) {
    throw py::error_already_set();
  }

  // Create a serialized message object
  rcl_serialized_message_t serialized_msg = rmw_get_zero_initialized_serialized_message();
  // Just copy pointer to avoid extra allocation and copy
  char * serialized_buffer;
  Py_ssize_t length;
  if (PYBIND11_BYTES_AS_STRING_AND_SIZE(pybuffer.ptr(), &serialized_buffer, &length)) {
    throw py::error_already_set();
  }
  if (length < 0) {
    throw py::error_already_set();
  }
  serialized_msg.buffer_capacity = length;
  serialized_msg.buffer_length = length;
  serialized_msg.buffer = reinterpret_cast<uint8_t *>(serialized_buffer);

  auto deserialized_ros_msg = create_from_py(pymsg_type);
  if (!deserialized_ros_msg) {
    throw py::error_already_set();
  }

  // Deserialize
  rmw_ret_t rmw_ret = rmw_deserialize(&serialized_msg, ts, deserialized_ros_msg.get());

  if (RMW_RET_OK != rmw_ret) {
    throw RMWError("failed to deserialize ROS message");
  }

  return convert_to_py_owned(std::move(deserialized_ros_msg), pymsg_type);
}
}  // namespace rclpy
