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

#ifndef RCLPY__TYPE_ADAPTER_HPP_
#define RCLPY__TYPE_ADAPTER_HPP_

#include <pybind11/pybind11.h>

#include <memory>
#include <thread>

#include "utils.hpp"

namespace py = pybind11;

namespace rclpy
{
/// The owner a type adapted publisher gives the rcl broker; message is filled only when read.
struct AdaptedMessage
{
  py::object value;
  py::object to_ros;
  py::object ros_type;
  std::unique_ptr<void, destroy_ros_message_function *> message;
  std::thread::id publisher_thread;
};

/// Whether this process has a type adapted Python publisher.
/**
 * The ROS form of its messages is filled by the first broker reader that needs it, which takes
 * the GIL inside a broker call: Python threads must not hold the GIL while they take.
 */
bool
adapted_messages_published();

/// Called when a type adapted Python publisher is created, before it publishes.
void
adapted_publisher_created();

/// Fill callback of rcl_publish_ipc_adapted for an AdaptedMessage.
bool
fill_adapted_message(void * message);

/// Release callback of rcl_publish_ipc_adapted for an AdaptedMessage.
void
release_adapted_message(void * message);

/// Rethrow on the publishing thread the Python error of a fill that ran on it.
void
rethrow_adapted_fill_error();

}  // namespace rclpy

#endif  // RCLPY__TYPE_ADAPTER_HPP_
