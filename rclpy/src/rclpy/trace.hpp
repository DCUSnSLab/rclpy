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

#ifndef RCLPY__TRACE_HPP_
#define RCLPY__TRACE_HPP_

#include <rcl/publisher.h>
#include <rcl/subscription.h>

// Exported functions that do nothing, for uprobes and LD_PRELOAD tracers. Bump the contract
// symbol whenever an anchor's arguments change.
extern "C"
{
__attribute__((visibility("default"))) int rclpy_trace_contract_1(void);
/// A taken message is now the Python object message.
__attribute__((visibility("default"))) void rclpy_trace_take_converted(
  const rcl_subscription_t * subscription, const void * message);
/// A publish begins, before the Python message is converted.
__attribute__((visibility("default"))) void rclpy_trace_publish_enter(
  const rcl_publisher_t * publisher);
}

#endif  // RCLPY__TRACE_HPP_
