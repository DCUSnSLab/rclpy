# Copyright 2026 nev
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
# writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from test import test_waitable

from rclpy.experimental import RclExecutor


class TestRclWaitable(test_waitable.TestWaitable):
    executor_type = RclExecutor


def test_timer_revalidates_after_backward_jump():
    from rclpy.clock import ROSClock
    from rclpy.context import Context
    from rclpy.impl.implementation_singleton import rclpy_implementation as _rclpy
    from rclpy.time import Time

    context = Context()
    context.init()
    clock = ROSClock()
    clock._set_ros_time_is_active(True)
    clock.set_ros_time_override(Time(seconds=1))
    core = _rclpy.RclExecutorCore()
    core.add_group(1, False)
    guard = _rclpy.GuardCondition(context.handle)
    timer = _rclpy.Timer(clock.handle, context.handle, 1_000_000_000, True)
    ids = [core.add(5, guard, 1, 1), core.add(1, timer, 2, 1)]
    try:
        guard.trigger_guard_condition()
        held = core.next()
        clock.set_ros_time_override(Time(seconds=2))
        core.wait(0)
        clock.set_ros_time_override(Time(seconds=.5))
        core.complete(held[1])
        item = core.next()
        if item:
            core.complete(item[1])
        assert item is None
    finally:
        for registration in ids:
            core.remove(registration)
        core.close()
        timer.destroy_when_not_in_use()
        guard.destroy_when_not_in_use()
        context.shutdown()
        context.destroy()


def test_post_returns_task():
    from rclpy.context import Context

    context = Context()
    context.init()
    executor = RclExecutor(context=context)
    try:
        future = executor.post(lambda value: value + 1, 65)
        executor.spin_until_future_complete(future, timeout_sec=1.)
        assert future.result() == 66
    finally:
        executor.shutdown()
        context.shutdown()
        context.destroy()


def test_node_transfer_detaches_previous_callbacks():
    from rclpy import create_node
    from rclpy.context import Context
    from test_msgs.msg import Empty

    context = Context()
    context.init()
    node = create_node('executor_transfer', context=context)
    first = RclExecutor(context=context)
    second = RclExecutor(context=context)
    future = second.create_future()
    node.create_subscription(Empty, 'executor_transfer', lambda msg: future.set_result(True), 10)
    publisher = node.create_publisher(Empty, 'executor_transfer', 10)
    try:
        first.add_node(node)
        first.spin_once(timeout_sec=0.)
        first.remove_node(node)
        second.add_node(node)
        second.spin_once(timeout_sec=0.)
        first.shutdown()
        publisher.publish(Empty())
        second.spin_until_future_complete(future, timeout_sec=1.)
        assert future.result() is True
    finally:
        first.shutdown()
        second.shutdown()
        node.destroy_node()
        context.shutdown()
        context.destroy()


def test_plain_subscription_dispatch():
    import threading

    import pytest
    from rclpy import create_node
    from rclpy.context import Context
    from test_msgs.msg import BasicTypes

    context = Context()
    context.init()
    node = create_node('executor_plain', context=context)
    executor = RclExecutor(context=context)
    received = []

    def plain(msg):
        received.append(msg.int32_value)
        if msg.int32_value == 2:
            raise RuntimeError('from callback')

    async def coroutine(msg):
        received.append(-msg.int32_value)

    sub = node.create_subscription(BasicTypes, 'executor_plain', plain, 10)
    publisher = node.create_publisher(BasicTypes, 'executor_plain', 10)
    executor.add_node(node)

    def spin_until(count):
        for _ in range(100):
            if len(received) >= count:
                return
            executor.spin_once(timeout_sec=0.05)

    try:
        executor.spin_once(timeout_sec=0.)
        for value in range(1, 4):
            publisher.publish(BasicTypes(int32_value=value))
        spin_until(1)
        assert received == [1]
        with pytest.raises(RuntimeError, match='from callback'):
            spin_until(2)
        spin_until(3)
        assert received == [1, 2, 3]
        # Arriving while the executor waits, it runs through the Python path.
        later = threading.Timer(0.1, lambda: publisher.publish(BasicTypes(int32_value=2)))
        later.start()
        with pytest.raises(RuntimeError, match='from callback'):
            executor.spin_once(timeout_sec=1.)
        later.join()
        publisher.publish(BasicTypes(int32_value=3))
        spin_until(5)
        assert received == [1, 2, 3, 2, 3]
        sub.callback = coroutine
        publisher.publish(BasicTypes(int32_value=4))
        spin_until(6)
        assert received == [1, 2, 3, 2, 3, -4]
    finally:
        executor.shutdown()
        node.destroy_node()
        context.shutdown()
        context.destroy()


def test_keep_all_messages_waiting_before_registration():
    import time

    from rclpy import create_node
    from rclpy.context import Context
    from rclpy.qos import HistoryPolicy, QoSProfile, ReliabilityPolicy
    from test_msgs.msg import Empty

    context = Context()
    context.init()
    node = create_node('executor_keep_all', context=context)
    qos = QoSProfile(history=HistoryPolicy.KEEP_ALL, reliability=ReliabilityPolicy.RELIABLE)
    received = []
    node.create_subscription(Empty, 'executor_keep_all', lambda msg: received.append(msg), qos)
    publisher = node.create_publisher(Empty, 'executor_keep_all', qos)
    executor = RclExecutor(context=context)
    try:
        for _ in range(5):
            publisher.publish(Empty())
        time.sleep(0.5)
        executor.add_node(node)
        end = time.monotonic() + 2.
        while len(received) < 5 and time.monotonic() < end:
            executor.spin_once(timeout_sec=0.1)
        assert len(received) == 5
    finally:
        executor.shutdown()
        node.destroy_node()
        context.shutdown()
        context.destroy()


def test_selection_policy_priority():
    from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
    from rclpy.context import Context
    from rclpy.experimental import SelectionPolicy
    from rclpy.node import Node

    context = Context()
    context.init()
    executor = RclExecutor(context=context, selection_policy=SelectionPolicy.PRIORITY)
    node = Node('priority_policy', context=context)
    low, high = MutuallyExclusiveCallbackGroup(), MutuallyExclusiveCallbackGroup()
    sequence = []
    timers = []
    for name, group in [('low', low), ('high', high)]:
        timer = node.create_timer(0.001, lambda n=name: sequence.append(n), callback_group=group)
        timers.append(timer)
    executor.add_node(node)
    executor.set_group_priority(low, -1)
    executor.set_group_priority(high, 10)
    import time
    time.sleep(0.01)
    executor.spin_once(timeout_sec=0)
    assert sequence == ['high']
    executor.shutdown()
    node.destroy_node()
    context.shutdown()
