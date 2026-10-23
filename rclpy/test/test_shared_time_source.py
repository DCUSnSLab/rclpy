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
from rclpy.executors import SingleThreadedExecutor
from rclpy.lifecycle import LifecycleNode
from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.time_source import CLOCK_TOPIC
import rosgraph_msgs.msg

SIM_TIME = [Parameter('use_sim_time', Parameter.Type.BOOL, True)]


def clock_threads():
    return [t for t in threading.enumerate() if t.name == 'rclpy_clock']


class TestSharedTimeSource(unittest.TestCase):
    """One /clock subscription for several nodes, ros2/rclcpp#2673."""

    def setUp(self):
        self.context = rclpy.context.Context()
        rclpy.init(context=self.context)
        self.observer = rclpy.create_node('shared_time_source_observer', context=self.context)
        self.stop = threading.Event()
        self.publisher = threading.Thread(target=self.publish_clock)
        self.publisher.start()

    def tearDown(self):
        self.stop.set()
        self.publisher.join()
        self.observer.destroy_node()
        self.context.try_shutdown()

    def publish_clock(self):
        pub = self.observer.create_publisher(rosgraph_msgs.msg.Clock, CLOCK_TOPIC, 10)
        msg = rosgraph_msgs.msg.Clock()
        msg.clock.sec = 100
        while not self.stop.is_set():
            msg.clock.nanosec = (msg.clock.nanosec + 10_000_000) % 1_000_000_000
            if msg.clock.nanosec == 0:
                msg.clock.sec += 1
            pub.publish(msg)
            time.sleep(0.01)

    def wait_for(self, condition, spin=None, timeout=10.):
        deadline = time.monotonic() + timeout
        while not condition():
            if time.monotonic() > deadline:
                return False
            if spin is not None:
                spin.spin_once(timeout_sec=0.01)
            else:
                time.sleep(0.01)
        return True

    def clock_subscriptions(self):
        return self.observer.count_subscribers(CLOCK_TOPIC)

    def test_nodes_share_one_clock_subscription(self):
        source = Node('time_source', context=self.context, parameter_overrides=SIM_TIME)
        sharing = [
            Node(f'sharing_{i}', context=self.context, time_source=source.get_time_source())
            for i in range(3)]
        sharing.append(LifecycleNode(
            'sharing_lifecycle', context=self.context, time_source=source.get_time_source()))
        executor = SingleThreadedExecutor(context=self.context)
        executor.add_node(source)
        try:
            self.assertTrue(self.wait_for(lambda: self.clock_subscriptions() == 1))
            for node in sharing:
                self.assertIs(source.get_time_source(), node.get_time_source())
                self.assertFalse(node.has_parameter('use_sim_time'))
                self.assertTrue(node.get_clock().ros_time_is_active)
            first = sharing[0].get_clock().now().nanoseconds
            self.assertTrue(self.wait_for(
                lambda: all(node.get_clock().now().nanoseconds > first for node in sharing),
                spin=executor))
            self.assertEqual(1, self.clock_subscriptions())
        finally:
            executor.shutdown()
            for node in sharing:
                node.destroy_node()
            source.destroy_node()

    def test_use_sim_time_of_the_source_switches_all_clocks(self):
        source = Node('time_source', context=self.context)
        node = Node('sharing', context=self.context, time_source=source.get_time_source())
        try:
            self.assertFalse(node.get_clock().ros_time_is_active)
            self.assertEqual(0, self.clock_subscriptions())
            source.set_parameters(SIM_TIME)
            self.assertTrue(node.get_clock().ros_time_is_active)
            self.assertTrue(self.wait_for(lambda: self.clock_subscriptions() == 1))
            source.set_parameters([Parameter('use_sim_time', Parameter.Type.BOOL, False)])
            self.assertFalse(node.get_clock().ros_time_is_active)
            self.assertTrue(self.wait_for(lambda: self.clock_subscriptions() == 0))
        finally:
            node.destroy_node()
            source.destroy_node()

    def test_sharing_node_destroyed_first(self):
        source = Node('time_source', context=self.context, parameter_overrides=SIM_TIME)
        node = Node('sharing', context=self.context, time_source=source.get_time_source())
        executor = SingleThreadedExecutor(context=self.context)
        executor.add_node(source)
        try:
            self.assertTrue(self.wait_for(
                lambda: node.get_clock().now().nanoseconds > 0, spin=executor))
            node.destroy_node()
            stopped = node.get_clock().now().nanoseconds
            first = source.get_clock().now().nanoseconds
            self.assertTrue(self.wait_for(
                lambda: source.get_clock().now().nanoseconds > first, spin=executor))
            self.assertEqual(stopped, node.get_clock().now().nanoseconds)
            self.assertEqual(1, self.clock_subscriptions())
        finally:
            executor.shutdown()
            source.destroy_node()

    def test_source_node_destroyed_first(self):
        source = Node(
            'time_source', context=self.context, parameter_overrides=SIM_TIME,
            use_clock_thread=True)
        first = Node('sharing_first', context=self.context, time_source=source.get_time_source())
        last = Node('sharing_last', context=self.context, time_source=source.get_time_source())
        self.assertEqual(1, len(clock_threads()))
        source.destroy_node()
        self.assertEqual(1, len(clock_threads()))
        start = last.get_clock().now().nanoseconds
        self.assertTrue(self.wait_for(lambda: last.get_clock().now().nanoseconds > start))
        first.destroy_node()
        self.assertEqual(1, len(clock_threads()))
        last.destroy_node()
        self.assertEqual([], clock_threads())
        self.assertTrue(self.wait_for(lambda: self.clock_subscriptions() == 0))

    def test_nodes_come_and_go_while_the_clock_thread_runs(self):
        source = Node(
            'time_source', context=self.context, parameter_overrides=SIM_TIME,
            use_clock_thread=True)
        try:
            for i in range(200):
                Node(f'sharing_{i}', context=self.context,
                     time_source=source.get_time_source()).destroy_node()
            self.assertEqual(1, len(clock_threads()))
            start = source.get_clock().now().nanoseconds
            self.assertTrue(self.wait_for(lambda: source.get_clock().now().nanoseconds > start))
        finally:
            source.destroy_node()
        self.assertEqual([], clock_threads())


if __name__ == '__main__':
    unittest.main()
