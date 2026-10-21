# Copyright 2026 nev
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
# writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

import threading
import time
import unittest

import rclpy
from rclpy.clock import Clock
from rclpy.clock_type import ClockType
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.time_source import CLOCK_TOPIC
import rosgraph_msgs.msg

SIM_TIME = [Parameter('use_sim_time', Parameter.Type.BOOL, True)]


def clock_threads():
    return [t for t in threading.enumerate() if t.name == 'rclpy_clock']


class TestTimeSourceClockThread(unittest.TestCase):
    """ROS time while callbacks run, ros2/rclpy#792."""

    def setUp(self):
        self.context = rclpy.context.Context()
        rclpy.init(context=self.context)
        self.publisher_node = rclpy.create_node('sim_clock', context=self.context)
        self.stop = threading.Event()
        self.publisher = threading.Thread(target=self.publish_clock)
        self.publisher.start()

    def tearDown(self):
        self.stop.set()
        self.publisher.join()
        self.publisher_node.destroy_node()
        self.context.try_shutdown()

    def publish_clock(self):
        pub = self.publisher_node.create_publisher(rosgraph_msgs.msg.Clock, CLOCK_TOPIC, 10)
        msg = rosgraph_msgs.msg.Clock()
        msg.clock.sec = 100
        while not self.stop.is_set():
            msg.clock.nanosec = (msg.clock.nanosec + 10_000_000) % 1_000_000_000
            if msg.clock.nanosec == 0:
                msg.clock.sec += 1
            pub.publish(msg)
            time.sleep(0.01)

    def wait_for_sim_time(self, node, timeout=5.0):
        deadline = time.monotonic() + timeout
        while node.get_clock().now().nanoseconds == 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        return node.get_clock().now().nanoseconds

    def test_clock_advances_during_callback_with_multithreaded_executor(self):
        node = Node('clock_user', context=self.context, parameter_overrides=SIM_TIME)
        executor = MultiThreadedExecutor(context=self.context)
        executor.add_node(node)
        advance = []

        def long_callback():
            if advance:
                return
            start = node.get_clock().now().nanoseconds
            time.sleep(0.5)
            advance.append(node.get_clock().now().nanoseconds - start)

        node.create_timer(0.1, long_callback, clock=Clock(clock_type=ClockType.STEADY_TIME))
        deadline = time.monotonic() + 5
        while not advance and time.monotonic() < deadline:
            executor.spin_once(timeout_sec=0.1)
        executor.shutdown()
        node.destroy_node()
        self.assertTrue(advance)
        self.assertGreater(advance[0], 200_000_000)

    def test_clock_thread_updates_time_without_spinning(self):
        node = Node(
            'clock_user', context=self.context, parameter_overrides=SIM_TIME,
            use_clock_thread=True)
        self.assertEqual(1, len(clock_threads()))
        self.assertNotIn(CLOCK_TOPIC, [sub.topic_name for sub in node.subscriptions])
        first = self.wait_for_sim_time(node)
        self.assertGreater(first, 0)
        time.sleep(0.3)
        self.assertGreater(node.get_clock().now().nanoseconds, first)
        node.destroy_node()
        self.assertEqual([], clock_threads())

    def test_clock_thread_follows_use_sim_time(self):
        node = Node('clock_user', context=self.context, use_clock_thread=True)
        self.assertEqual([], clock_threads())
        node.set_parameters(SIM_TIME)
        self.assertEqual(1, len(clock_threads()))
        self.assertGreater(self.wait_for_sim_time(node), 0)
        node.set_parameters([Parameter('use_sim_time', Parameter.Type.BOOL, False)])
        self.assertEqual([], clock_threads())
        node.destroy_node()

    def test_clock_thread_stops_on_shutdown(self):
        Node(
            'clock_user', context=self.context, parameter_overrides=SIM_TIME,
            use_clock_thread=True)
        self.assertEqual(1, len(clock_threads()))
        self.stop.set()
        self.publisher.join()
        self.context.try_shutdown()
        self.assertEqual([], clock_threads())


if __name__ == '__main__':
    unittest.main()
