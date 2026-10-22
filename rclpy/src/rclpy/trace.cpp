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

#include "trace.hpp"

#if defined(__GNUC__) && !defined(__clang__)
#define RCLPY_TRACE_NOINLINE __attribute__((noinline, noipa))
#else
#define RCLPY_TRACE_NOINLINE __attribute__((noinline))
#endif

extern "C"
{
int rclpy_trace_contract_1(void) {return 1;}

RCLPY_TRACE_NOINLINE void rclpy_trace_take_converted(
  const rcl_subscription_t * subscription, const void * message)
{
  asm volatile ("" : : "g" (subscription), "g" (message) : "memory");
}

RCLPY_TRACE_NOINLINE void rclpy_trace_publish_enter(const rcl_publisher_t * publisher)
{
  asm volatile ("" : : "g" (publisher) : "memory");
}
}
