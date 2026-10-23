# Copyright 2026 nev
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
# writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from test import test_waitable

import pytest
from rclpy.experimental import RclExecutor
from rclpy.experimental import SelectionPolicy


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


def test_thread_start_on_spinning_thread():
    import threading

    from rclpy.context import Context

    context = Context()
    context.init()
    executor = RclExecutor(context=context)
    starts = []
    executor.on_thread_start(lambda index, group: starts.append(
        (index, group, threading.get_ident())))
    executor.post(executor.shutdown)
    executor.spin()
    assert starts == [(0, None, threading.get_ident())]
    context.shutdown()
    context.destroy()


def test_partition_threads_isolation_and_dynamic_groups():
    import threading
    import time

    import rclpy
    from rclpy.callback_groups import MutuallyExclusiveCallbackGroup, ReentrantCallbackGroup
    from rclpy.context import Context
    import pytest

    context = Context()
    context.init()
    node = rclpy.create_node('python_partition', context=context)
    executor = RclExecutor(context=context, partitioned=True)
    slow = MutuallyExclusiveCallbackGroup()
    fast = MutuallyExclusiveCallbackGroup()
    starts = {}
    calls = {}
    entered = threading.Event()
    finished = threading.Event()
    fast_done = threading.Event()
    errors = []

    def start(index, group):
        assert group not in starts
        starts[group] = (index, threading.get_ident())

    def slow_callback():
        slow_timer.cancel()
        calls.setdefault(slow, set()).add(threading.get_ident())
        entered.set()
        time.sleep(.2)
        finished.set()

    def fast_callback():
        calls.setdefault(fast, set()).add(threading.get_ident())
        fast_done.set()

    slow_timer = node.create_timer(.001, slow_callback, callback_group=slow)
    fast_timer = node.create_timer(.002, fast_callback, callback_group=fast)
    executor.on_thread_start(start)
    executor.add_node(node)

    def spin():
        try:
            executor.spin()
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=spin)
    worker.start()
    try:
        assert entered.wait(2.)
        begin = time.monotonic()
        assert fast_done.wait(.1)
        assert time.monotonic() - begin < .1
        assert not finished.is_set()
        fast_timer.cancel()
        node.destroy_timer(fast_timer)
        late = MutuallyExclusiveCallbackGroup()
        late_done = threading.Event()

        def late_callback():
            late_timer.cancel()
            calls.setdefault(late, set()).add(threading.get_ident())
            late_done.set()

        late_timer = node.create_timer(.001, late_callback, callback_group=late)
        assert late_done.wait(1.)
        node.destroy_timer(late_timer)
        assert finished.wait(1.)
        for group in (slow, fast, late):
            assert calls[group] == {starts[group][1]}
        assert starts[slow][1] != starts[fast][1]
        other = rclpy.create_node('python_reentrant', context=context)
        other.create_timer(1., lambda: None, callback_group=ReentrantCallbackGroup())
        try:
            with pytest.raises(ValueError, match='Reentrant'):
                executor.add_node(other)
        finally:
            other.destroy_node()
    finally:
        executor.shutdown()
        worker.join(2.)
        assert not worker.is_alive()
        node.destroy_node()
        context.shutdown()
        context.destroy()
    assert not errors


def test_partition_coroutine_resumes_on_its_group_thread():
    import threading

    import rclpy
    from rclpy.context import Context

    context = Context()
    context.init()
    node = rclpy.create_node('partition_coroutine', context=context)
    executor = RclExecutor(context=context, partitioned=True)
    future = executor.create_future()
    entered = threading.Event()
    done = threading.Event()
    threads = []

    async def callback():
        timer.cancel()
        threads.append(threading.get_ident())
        entered.set()
        await future
        threads.append(threading.get_ident())
        done.set()

    timer = node.create_timer(.001, callback)
    executor.add_node(node)
    worker = threading.Thread(target=executor.spin)
    worker.start()
    try:
        assert entered.wait(2.)
        task = executor.create_task(lambda: future.set_result(True))
        assert done.wait(2.)
        assert task.done()
        assert len(threads) == 2
        assert threads[0] == threads[1]
        assert threads[0] != threading.get_ident()
    finally:
        executor.shutdown()
        worker.join(2.)
        assert not worker.is_alive()
        node.destroy_node()
        context.shutdown()
        context.destroy()


def test_partition_posted_coroutine_leaves_the_control_worker_available():
    import threading

    from rclpy.context import Context

    context = Context()
    context.init()
    executor = RclExecutor(context=context, partitioned=True)
    future = executor.create_future()
    entered = threading.Event()
    done = threading.Event()
    threads = []

    async def callback():
        threads.append(threading.get_ident())
        entered.set()
        await future
        threads.append(threading.get_ident())
        done.set()

    task = executor.post(callback)
    worker = threading.Thread(target=executor.spin)
    worker.start()
    try:
        assert entered.wait(2.)
        executor.post(future.set_result, True)
        assert done.wait(2.)
        assert task.done()
        assert threads[0] == threads[1]
    finally:
        executor.shutdown()
        worker.join(2.)
        assert not worker.is_alive()
        context.shutdown()
        context.destroy()


@pytest.mark.parametrize('policy', [SelectionPolicy.FIFO, SelectionPolicy.WAIT_SET])
def test_pipe_source_orders_with_ros_and_removes_pending_work(policy):
    import os
    import threading
    import time

    import rclpy
    from rclpy.context import Context
    from test_msgs.msg import Empty

    context = Context()
    context.init()
    node = rclpy.create_node('external_pipe', context=context)
    executor = RclExecutor(context=context, selection_policy=policy)
    sequence = []
    node.create_subscription(Empty, 'external_order', lambda _: sequence.append('ros'), 10)
    publisher = node.create_publisher(Empty, 'external_order', 10)
    executor.add_node(node)
    source = executor.add_source(node.default_callback_group, lambda: sequence.append('external'))
    reader, writer = os.pipe()
    try:
        def driver():
            os.read(reader, 1)
            executor.notify_source(source)

        thread = threading.Thread(target=driver)
        thread.start()
        os.write(writer, b'x')
        thread.join()
        publisher.publish(Empty())
        time.sleep(.05)
        executor.spin_some()
        expected = ['external', 'ros'] if policy == SelectionPolicy.FIFO else ['ros', 'external']
        assert sequence == expected
        executor.notify_source(source)
        executor.remove_source(source)
        executor.notify_source(source)
        executor.spin_some()
        assert len(sequence) == 2
    finally:
        os.close(reader)
        os.close(writer)
        executor.shutdown()
        node.destroy_node()
        context.shutdown()
        context.destroy()


def test_source_runs_once_per_notification_and_raises_from_spin():
    from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
    from rclpy.context import Context

    context = Context()
    context.init()
    executor = RclExecutor(context=context)
    calls = []
    source = executor.add_source(MutuallyExclusiveCallbackGroup(), lambda: calls.append(1))

    def fail():
        raise RuntimeError('source failed')

    failing = executor.add_source(MutuallyExclusiveCallbackGroup(), fail)
    try:
        executor.notify_source(source, 3)
        for _ in range(3):
            executor.spin_once(timeout_sec=1.)
        assert calls == [1, 1, 1]
        executor.notify_source(failing)
        with pytest.raises(RuntimeError, match='source failed'):
            executor.spin_once(timeout_sec=1.)
    finally:
        executor.shutdown()
        context.shutdown()
        context.destroy()


def test_coroutine_source_holds_its_exclusive_group():
    from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
    from rclpy.context import Context

    context = Context()
    context.init()
    executor = RclExecutor(context=context)
    group = MutuallyExclusiveCallbackGroup()
    future = executor.create_future()
    sequence = []

    async def waiting():
        sequence.append('enter')
        await future
        sequence.append('leave')

    first = executor.add_source(group, waiting)
    second = executor.add_source(group, lambda: sequence.append('second'))
    try:
        executor.notify_source(first)
        executor.spin_once(timeout_sec=1.)
        executor.notify_source(second)
        executor.spin_once(timeout_sec=.1)
        assert sequence == ['enter']
        future.set_result(True)
        executor.spin_until_future_complete(future, timeout_sec=1.)
        for _ in range(3):
            executor.spin_once(timeout_sec=.1)
        assert sequence == ['enter', 'leave', 'second']
    finally:
        executor.shutdown()
        context.shutdown()
        context.destroy()


def test_partition_source_runs_on_its_group_thread():
    import threading

    from rclpy.callback_groups import MutuallyExclusiveCallbackGroup, ReentrantCallbackGroup
    from rclpy.context import Context

    context = Context()
    context.init()
    executor = RclExecutor(context=context, partitioned=True)
    starts = {}
    executor.on_thread_start(
        lambda index, group: starts.__setitem__(group, threading.get_ident()))
    worker = threading.Thread(target=executor.spin)
    worker.start()
    try:
        late = MutuallyExclusiveCallbackGroup()
        threads = []
        done = threading.Event()

        def callback():
            threads.append(threading.get_ident())
            done.set()

        source = executor.add_source(late, callback)
        removed = executor.add_source(MutuallyExclusiveCallbackGroup(), lambda: threads.append(0))
        executor.notify_source(removed)
        executor.remove_source(removed)
        executor.notify_source(source)
        assert done.wait(2.)
        assert threads == [starts[late]]
        assert threads[0] != threading.get_ident()
        executor.remove_source(source)
        with pytest.raises(ValueError, match='Reentrant'):
            executor.add_source(ReentrantCallbackGroup(), lambda: None)
    finally:
        executor.shutdown()
        worker.join(2.)
        assert not worker.is_alive()
        context.shutdown()
        context.destroy()
