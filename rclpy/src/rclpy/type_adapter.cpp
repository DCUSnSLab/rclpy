// Copyright 2026 nev
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

#include "type_adapter.hpp"

#include <pybind11/pybind11.h>
#include <rcl/error_handling.h>

#include <atomic>
#include <memory>
#include <thread>
#include <utility>

namespace rclpy
{
namespace
{
std::atomic<bool> published{false};
thread_local std::unique_ptr<py::error_already_set> fill_error;

// Broker readers in other languages can release a message after the interpreter is finalized.
bool
python_alive()
{
#if PY_VERSION_HEX >= 0x030D0000
  return Py_IsInitialized() && !Py_IsFinalizing();
#else
  return Py_IsInitialized() && !_Py_IsFinalizing();
#endif
}
}  // namespace

bool
adapted_messages_published()
{
  return published.load(std::memory_order_acquire);
}

void
adapted_publisher_created()
{
  published.store(true, std::memory_order_release);
}

bool
fill_adapted_message(void * pointer)
{
  auto adapted = static_cast<AdaptedMessage *>(pointer);
  if (!python_alive()) {
    return false;
  }
  py::gil_scoped_acquire gil;
  try {
    py::object ros = adapted->to_ros(adapted->value);
    if (!py::isinstance(ros, adapted->ros_type)) {
      PyErr_Format(
        PyExc_TypeError, "to_ros returned %R, expected an instance of %R",
        Py_TYPE(ros.ptr()), adapted->ros_type.ptr());
      throw py::error_already_set();
    }
    convert_from_py(ros, adapted->message.get());
    return true;
  } catch (py::error_already_set & error) {
    if (adapted->publisher_thread == std::this_thread::get_id()) {
      fill_error = std::make_unique<py::error_already_set>(std::move(error));
    } else {
      error.discard_as_unraisable("converting a type adapted message for a broker reader");
    }
  }
  return false;
}

void
release_adapted_message(void * pointer)
{
  std::unique_ptr<AdaptedMessage> adapted(static_cast<AdaptedMessage *>(pointer));
  if (!python_alive()) {
    adapted->value.release();
    adapted->to_ros.release();
    adapted->ros_type.release();
    return;
  }
  py::gil_scoped_acquire gil;
  adapted.reset();
}

void
rethrow_adapted_fill_error()
{
  if (fill_error) {
    rcl_reset_error();
    auto error = std::move(*fill_error);
    fill_error.reset();
    throw error;
  }
}

}  // namespace rclpy
