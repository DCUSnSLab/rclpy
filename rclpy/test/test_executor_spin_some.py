# Copyright 2026 nev
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
# writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

import time
import unittest

import rclpy
from rclpy.executors import MultiThreadedExecutor
from rclpy.executors import SingleThreadedExecutor
from rclpy.experimental import RclExecutor
from rclpy.qos import QoSProfile
from test_msgs.msg import Empty


class TestExecutorSpinSome(unittest.TestCase):
    """rclpy spin_some, ros2/rclpy#486."""

    def setUp(self):
        self.context = rclpy.context.Context()
        rclpy.init(context=self.context)

    def tearDown(self):
        rclpy.shutdown(context=self.context)

    def for_each_executor(self, body):
        for executor_type in (SingleThreadedExecutor, MultiThreadedExecutor, RclExecutor):
            with self.subTest(executor=executor_type.__name__):
                node = rclpy.create_node('spin_some', context=self.context)
                executor = executor_type(context=self.context)
                try:
                    body(node, executor)
                finally:
                    executor.shutdown()
                    node.destroy_node()

    def subscribe(self, node, topic, callback):
        node.create_subscription(Empty, topic, callback, QoSProfile(depth=10))
        publisher = node.create_publisher(Empty, topic, QoSProfile(depth=10))
        deadline = time.monotonic() + 5
        while publisher.get_subscription_count() == 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        return publisher

    def test_runs_each_ready_callback_once(self):
        def body(node, executor):
            calls = {'a': 0, 'b': 0, 'timer': 0}

            def count(key):
                def callback(*args):
                    calls[key] += 1
                return callback

            pub_a = self.subscribe(node, 'a', count('a'))
            pub_b = self.subscribe(node, 'b', count('b'))
            node.create_timer(0.01, count('timer'))
            executor.add_node(node)
            for _ in range(3):
                pub_a.publish(Empty())
            pub_b.publish(Empty())
            time.sleep(0.3)

            executor.spin_some()
            self.assertEqual({'a': 1, 'b': 1, 'timer': 1}, calls)
            executor.spin_some()
            self.assertEqual(2, calls['a'])
        self.for_each_executor(body)

    def test_leaves_work_that_became_ready_to_the_next_call(self):
        def body(node, executor):
            calls = []
            publisher = None

            def callback(msg):
                calls.append(msg)
                publisher.publish(Empty())
                time.sleep(0.3)

            publisher = self.subscribe(node, 'echo', callback)
            executor.add_node(node)
            publisher.publish(Empty())
            time.sleep(0.3)

            executor.spin_some()
            self.assertEqual(1, len(calls))
            executor.spin_some()
            self.assertEqual(2, len(calls))
        self.for_each_executor(body)

    def test_returns_without_work(self):
        def body(node, executor):
            executor.add_node(node)
            start = time.monotonic()
            executor.spin_some()
            self.assertLess(time.monotonic() - start, 0.5)
        self.for_each_executor(body)

    def test_max_duration(self):
        def body(node, executor):
            calls = []

            def slow():
                calls.append(1)
                time.sleep(0.2)

            for _ in range(3):
                node.create_timer(0.01, slow)
            executor.add_node(node)
            time.sleep(0.05)

            executor.spin_some(max_duration_sec=0.1)
            self.assertEqual(1, len(calls))
        self.for_each_executor(body)

    def test_raises_callback_exception(self):
        def body(node, executor):
            def fail():
                raise RuntimeError('from timer')

            node.create_timer(0.01, fail)
            executor.add_node(node)
            time.sleep(0.05)

            with self.assertRaises(RuntimeError):
                executor.spin_some()
        self.for_each_executor(body)


if __name__ == '__main__':
    unittest.main()
