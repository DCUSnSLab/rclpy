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
#include <rcl/publisher.h>
#include <rosidl_runtime_c/message_type_support_struct.h>
#include <rmw/rmw.h>
#include <rmw/serialized_message.h>

#include <memory>
#include <string>
#include <utility>

#include "rcl/intra_process.h"

#include "exceptions.hpp"
#include "node.hpp"
#include "publisher.hpp"
#include "trace.hpp"
#include "utils.hpp"

namespace rclpy
{
namespace
{
/// A message lent by the middleware, returned unless it was published.
struct PublisherLoan
{
  PublisherLoan(std::shared_ptr<rcl_publisher_t> publisher, void * message)
  : publisher(std::move(publisher)), message(message) {}

  ~PublisherLoan()
  {
    if (published) {
      return;
    }
    // The rmw call, unlike rcl's, also works after shutdown. Some middlewares cannot finalize
    // a publisher that still lends messages.
    rmw_publisher_t * rmw_publisher = rcl_publisher_get_rmw_handle(publisher.get());
    if (!rmw_publisher ||
      RMW_RET_OK != rmw_return_loaned_message_from_publisher(rmw_publisher, message))
    {
      RCUTILS_SAFE_FWRITE_TO_STDERR("[rclpy] failed to return a loaned message: ");
      RCUTILS_SAFE_FWRITE_TO_STDERR(rcutils_get_error_string().str);
      RCUTILS_SAFE_FWRITE_TO_STDERR("\n");
      rcutils_reset_error();
    }
  }

  std::shared_ptr<rcl_publisher_t> publisher;
  void * message;
  bool published = false;
  // Live LoanBuffer objects, each the base of NumPy views of message.
  size_t buffers = 0;
};

/// Base object of the NumPy views of a loan. Its writable buffer makes the views writable.
struct LoanBuffer
{
  explicit LoanBuffer(std::shared_ptr<PublisherLoan> loan)
  : loan(std::move(loan)) {++this->loan->buffers;}

  ~LoanBuffer() {--loan->buffers;}

  std::shared_ptr<PublisherLoan> loan;
};

bool
is_message(py::handle value)
{
  return py::hasattr(py::type::handle_of(value), "_TYPE_SUPPORT");
}

/// Delete the fields of pymsg, and of the messages in it, that are views of loan.
void
detach_views(py::handle pymsg, const PublisherLoan * loan)
{
  for (auto name : py::type::handle_of(pymsg).attr("__slots__")) {
    PyObject * value = PyObject_GetAttr(pymsg.ptr(), name.ptr());
    if (value == nullptr) {
      PyErr_Clear();
      continue;
    }
    auto field = py::reinterpret_steal<py::object>(value);
    if (PyLong_Check(value) || PyFloat_Check(value)) {
      continue;
    }
    if (PyObject_CheckBuffer(value)) {
      auto base = py::getattr(field, "base", py::none());
      if (py::isinstance<LoanBuffer>(base) &&
        base.cast<const LoanBuffer &>().loan.get() == loan &&
        PyObject_DelAttr(pymsg.ptr(), name.ptr()) < 0)
      {
        throw py::error_already_set();
      }
    } else if (PyList_Check(value)) {
      for (auto item : field) {
        if (is_message(item)) {
          detach_views(item, loan);
        }
      }
    } else if (is_message(field)) {
      detach_views(field, loan);
    }
  }
}
}  // namespace

Publisher::Publisher(
  Node & node, py::object pymsg_type, std::string topic,
  py::object pyqos_profile)
: node_(node)
{
  auto msg_type = static_cast<rosidl_message_type_support_t *>(
    common_get_type_support(pymsg_type));
  if (!msg_type) {
    throw py::error_already_set();
  }

  rcl_publisher_options_t publisher_ops = rcl_publisher_get_default_options();

  if (!pyqos_profile.is_none()) {
    publisher_ops.qos = pyqos_profile.cast<rmw_qos_profile_t>();
  }

  rcl_publisher_ = std::shared_ptr<rcl_publisher_t>(
    new rcl_publisher_t,
    [node](rcl_publisher_t * publisher)
    {
      // Intentionally capturing node by value so shared_ptr can be transferred to copies
      rcl_ret_t ret = rcl_publisher_fini(publisher, node.rcl_ptr());
      if (RCL_RET_OK != ret) {
        // Warning should use line number of the current stack frame
        int stack_level = 1;
        PyErr_WarnFormat(
          PyExc_RuntimeWarning, stack_level, "Failed to fini publisher: %s",
          rcl_get_error_string().str);
        rcl_reset_error();
      }
      delete publisher;
    });

  *rcl_publisher_ = rcl_get_zero_initialized_publisher();

  rcl_ret_t ret = rcl_publisher_init(
    rcl_publisher_.get(), node_.rcl_ptr(), msg_type,
    topic.c_str(), &publisher_ops);
  if (RCL_RET_OK != ret) {
    if (RCL_RET_TOPIC_NAME_INVALID == ret) {
      std::string error_text{"Failed to create publisher due to invalid topic name '"};
      error_text += topic;
      error_text += "'";
      throw py::value_error(error_text);
    }
    throw RCLError("Failed to create publisher");
  }
}

void Publisher::destroy()
{
  rcl_publisher_.reset();
  node_.destroy();
}

const char *
Publisher::get_logger_name()
{
  const char * node_logger_name = rcl_node_get_logger_name(node_.rcl_ptr());
  if (!node_logger_name) {
    throw RCLError("Node logger name not set");
  }

  return node_logger_name;
}

size_t
Publisher::get_subscription_count()
{
  size_t count = 0;
  rcl_ret_t ret = rcl_publisher_get_subscription_count(rcl_publisher_.get(), &count);
  if (RCL_RET_OK != ret) {
    throw RCLError("failed to get subscription count");
  }

  return count;
}

std::string
Publisher::get_topic_name()
{
  const char * topic_name = rcl_publisher_get_topic_name(rcl_publisher_.get());
  if (!topic_name) {
    throw RCLError("failed to get topic name");
  }

  return std::string(topic_name);
}

void
Publisher::publish(py::object pymsg)
{
  rclpy_trace_publish_enter(rcl_publisher_.get());
  auto raw_ros_message = convert_from_py(pymsg);
  if (!raw_ros_message) {
    throw py::error_already_set();
  }

  rcl_ret_t ret;
  if (rcl_publisher_is_ipc_enabled(rcl_publisher_.get())) {
    auto owner = new decltype(raw_ros_message)(std::move(raw_ros_message));
    ret = rcl_publish_ipc_owned(rcl_publisher_.get(), owner->get(),
        [](void * value) {delete static_cast<decltype(owner)>(value);}, owner);
  } else {
    ret = rcl_publish(rcl_publisher_.get(), raw_ros_message.get(), NULL);
  }
  if (RCL_RET_OK != ret) {
    throw RCLError("Failed to publish");
  }
}

bool
Publisher::can_loan_messages() const
{
  return rcl_publisher_can_loan_messages(rcl_publisher_.get());
}

py::tuple
Publisher::borrow_loaned_message(py::object pymsg_type)
{
  auto type_support = static_cast<rosidl_message_type_support_t *>(
    common_get_type_support(pymsg_type));
  if (!type_support) {
    throw py::error_already_set();
  }
  void * message = nullptr;
  if (RCL_RET_OK != rcl_borrow_loaned_message(rcl_publisher_.get(), type_support, &message)) {
    throw RCLError("Failed to borrow a loaned message");
  }
  auto loan = std::make_shared<PublisherLoan>(rcl_publisher_, message);
  py::object pymsg;
  {
    // Only the views keep the buffer alive, so PublisherLoan::buffers counts the live views.
    py::object buffer = py::cast(std::make_unique<LoanBuffer>(loan));
    pymsg = convert_to_py_with_owner(message, buffer, pymsg_type);
  }
  return py::make_tuple(py::cast(loan), pymsg);
}

bool
Publisher::publish_loaned_message(py::object pyloan, py::object pymsg)
{
  rclpy_trace_publish_enter(rcl_publisher_.get());
  auto loan = pyloan.cast<std::shared_ptr<PublisherLoan>>();
  if (loan->published) {
    throw py::value_error("the loaned message was already published");
  }
  convert_from_py(pymsg, loan->message);
  detach_views(pymsg, loan.get());
  if (loan->buffers > 0) {
    // Python still references the loan, which the middleware would own once published.
    if (RCL_RET_OK != rcl_publish(rcl_publisher_.get(), loan->message, NULL)) {
      throw RCLError("Failed to publish");
    }
    return false;
  }
  if (RCL_RET_OK != rcl_publish_loaned_message(rcl_publisher_.get(), loan->message, NULL)) {
    throw RCLError("Failed to publish");
  }
  loan->published = true;
  return true;
}

void
Publisher::publish_raw(std::string msg)
{
  rcl_serialized_message_t serialized_msg = rmw_get_zero_initialized_serialized_message();
  serialized_msg.buffer_capacity = msg.size();
  serialized_msg.buffer_length = msg.size();
  serialized_msg.buffer = reinterpret_cast<uint8_t *>(const_cast<char *>(msg.c_str()));

  rcl_ret_t ret = rcl_publish_serialized_message(rcl_publisher_.get(), &serialized_msg, NULL);
  if (RCL_RET_OK != ret) {
    throw RCLError("Failed to publish");
  }
}

bool
Publisher::wait_for_all_acked(rcl_duration_t pytimeout)
{
  rcl_ret_t ret = rcl_publisher_wait_for_all_acked(rcl_publisher_.get(), pytimeout.nanoseconds);
  if (RCL_RET_OK == ret) {
    return true;
  } else if (RCL_RET_TIMEOUT == ret) {
    return false;
  }
  throw RCLError("Failed to wait for all acknowledgements");
}

void
define_publisher(py::object module)
{
  py::class_<PublisherLoan, std::shared_ptr<PublisherLoan>>(module, "PublisherLoan");
  py::class_<LoanBuffer>(module, "PublisherLoanBuffer", py::buffer_protocol())
  .def_buffer(
    [](LoanBuffer & buffer) {
      return py::buffer_info(buffer.loan->message, 1, "B", 0, false);
    });
  py::class_<Publisher, Destroyable, std::shared_ptr<Publisher>>(module, "Publisher")
  .def(py::init<Node &, py::object, std::string, py::object>())
  .def_property_readonly(
    "pointer", [](const Publisher & publisher) {
      return reinterpret_cast<size_t>(publisher.rcl_ptr());
    },
    "Get the address of the entity as an integer")
  .def(
    "get_logger_name", &Publisher::get_logger_name,
    "Get the name of the logger associated with the node of the publisher")
  .def(
    "get_subscription_count", &Publisher::get_subscription_count,
    "Count subscribers from a publisher.")
  .def(
    "get_topic_name", &Publisher::get_topic_name,
    "Retrieve the topic name from a Publisher.")
  .def(
    "publish", &Publisher::publish,
    "Publish a message")
  .def(
    "publish_raw", &Publisher::publish_raw,
    "Publish a serialized message.")
  .def(
    "can_loan_messages", &Publisher::can_loan_messages,
    "Whether the middleware lends messages to this publisher.")
  .def(
    "borrow_loaned_message", &Publisher::borrow_loaned_message,
    "Borrow a message from the middleware.")
  .def(
    "publish_loaned_message", &Publisher::publish_loaned_message,
    "Publish a borrowed message.")
  .def(
    "wait_for_all_acked", &Publisher::wait_for_all_acked,
    "Wait until all published message data is acknowledged");
}
}  // namespace rclpy
