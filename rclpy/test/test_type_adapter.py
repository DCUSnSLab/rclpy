# Copyright 2020 Open Source Robotics Foundation, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import gc
import os
import subprocess
import sys
import threading
import time
from unittest.mock import Mock
import weakref

import numpy
import pytest

import rclpy
from rclpy.context import Context
from rclpy.executors import SingleThreadedExecutor
from rclpy.type_adapter import create_publisher
from rclpy.type_adapter import create_subscription
from rclpy.type_adapter import register_type_adapter
from rclpy.type_adapter import TypeAdapter
from std_msgs.msg import Float64MultiArray
from std_msgs.msg import String


def test_registration_and_validation():
    class Value:
        pass

    adapter = TypeAdapter(Value, String, lambda value: String(), lambda message: Value())
    register_type_adapter(adapter)
    register_type_adapter(adapter)
    node = Mock()
    publisher = create_publisher(node, Value, 'adapted', 1)
    publisher.publish(Value())
    assert node.create_publisher.call_args.args == (adapter, 'adapted', 1)
    assert isinstance(publisher.ros_publisher.publish.call_args.args[0], Value)
    with pytest.raises(TypeError):
        publisher.publish(object())
    with pytest.raises(ValueError):
        register_type_adapter(TypeAdapter(Value, String, str, str))
    with pytest.raises(TypeError):
        create_publisher(node, object, 'adapted', 1)
    with pytest.raises(TypeError):
        create_publisher(node, object, 'adapted', 1, adapter=adapter)
    with pytest.raises(ValueError):
        create_subscription(node, Value, 'adapted', lambda value: None, 1, raw=True)


@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('with_info', [False, True])
def test_numpy_wire_interoperability(asynchronous, with_info):
    context = Context()
    rclpy.init(context=context)
    node = rclpy.create_node('test_type_adapter', context=context)
    executor = SingleThreadedExecutor(context=context)
    executor.add_node(node)
    calls = {'to_ros': 0, 'from_ros': 0}

    def to_ros(value):
        calls['to_ros'] += 1
        return Float64MultiArray(data=value)

    def from_ros(message):
        calls['from_ros'] += 1
        return numpy.asarray(message.data)

    adapter = TypeAdapter(numpy.ndarray, Float64MultiArray, to_ros, from_ros)
    received = []
    wire_received = []

    def record(value, *info):
        assert isinstance(value, numpy.ndarray)
        assert len(info) == int(with_info)
        received.append(value)

    if with_info:
        def consume(value, info):
            record(value, info)
    else:
        def consume(value):
            record(value)
    if asynchronous:
        sync_consume = consume
        if with_info:
            async def consume(value, info):
                sync_consume(value, info)
        else:
            async def consume(value):
                sync_consume(value)
    try:
        publisher = create_publisher(node, numpy.ndarray, 'adapted', 10, adapter=adapter)
        subscription = create_subscription(
            node, numpy.ndarray, 'adapted', consume, 10, adapter=adapter)
        wire_subscription = node.create_subscription(
            Float64MultiArray, 'adapted', wire_received.append, 10)
        wire_publisher = node.create_publisher(Float64MultiArray, 'adapted', 10)
        deadline = time.monotonic() + 5
        while publisher.ros_publisher.get_subscription_count() < 2:
            assert time.monotonic() < deadline
            executor.spin_once(timeout_sec=0.01)
        value = numpy.arange(50, dtype=numpy.float64)
        publisher.publish(value)
        wire_publisher.publish(Float64MultiArray(data=value))
        while len(received) < 2 or len(wire_received) < 2:
            assert time.monotonic() < deadline
            executor.spin_once(timeout_sec=0.01)
        # The broker hands the published array itself to the adapted subscription.
        broker = BROKER and os.environ.get('RCL_INTRA_PROCESS') == '1'
        assert calls == {'to_ros': 1, 'from_ros': 1 if broker else 2}
        for result in received:
            numpy.testing.assert_array_equal(result, value)
        assert node.destroy_publisher(publisher.ros_publisher)
        assert node.destroy_publisher(wire_publisher)
        assert node.destroy_subscription(subscription)
        assert node.destroy_subscription(wire_subscription)
    finally:
        executor.shutdown()
        node.destroy_node()
        context.shutdown()
    numpy.testing.assert_array_equal(received[0], value)


class Text:

    def __init__(self, text, converted=False):
        self.text = text
        self.converted = converted


class Counted:
    """Conversions of a Text adapter that count their calls."""

    def __init__(self):
        self.to_ros_calls = 0
        self.from_ros_calls = 0
        self.to_ros_threads = set()

    def to_ros(self, value):
        self.to_ros_calls += 1
        self.to_ros_threads.add(threading.get_ident())
        return String(data=value.text)

    def from_ros(self, message):
        self.from_ros_calls += 1
        return Text(message.data, converted=True)

    def adapter(self):
        return TypeAdapter(Text, String, self.to_ros, self.from_ros)


BROKER = os.environ.get('RMW_IMPLEMENTATION') == 'rmw_zenoh_cpp'


@pytest.fixture(params=[False, True], ids=['rmw', 'broker'])
def broker_context(request, monkeypatch):
    monkeypatch.setenv('RCL_INTRA_PROCESS', '1' if request.param else '0')
    context = Context()
    rclpy.init(context=context)
    yield context, request.param and BROKER
    context.try_shutdown()


def spin_until(executor, condition, timeout=5.):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline
        executor.spin_once(timeout_sec=0.01)


def wait_for_subscriptions(executor, publisher, count):
    spin_until(executor, lambda: publisher.get_subscription_count() >= count)


@pytest.mark.parametrize('executor_type', ['single', 'rcl', 'events'])
def test_node_endpoints_deliver_custom_objects(broker_context, executor_type):
    from rclpy.experimental import EventsExecutor, RclExecutor
    context, broker = broker_context
    node = rclpy.create_node('adapted_node_api', context=context)
    executor = {'single': SingleThreadedExecutor, 'rcl': RclExecutor,
                'events': EventsExecutor}[executor_type](context=context)
    counted = Counted()
    adapter = counted.adapter()
    adapted, ros, infos = [], [], []
    try:
        publisher = node.create_publisher(adapter, 'adapted_api', 10)
        assert publisher.type_adapter is adapter and publisher.msg_type is String
        subscription = node.create_subscription(
            adapter, 'adapted_api',
            lambda value, info: (adapted.append(value), infos.append(info)), 10)
        assert subscription.type_adapter is adapter and subscription.msg_type is String
        node.create_subscription(String, 'adapted_api', ros.append, 10)
        executor.add_node(node)
        wait_for_subscriptions(executor, publisher, 2)
        sent = Text('first')
        publisher.publish(sent)
        publisher.publish(String(data='second'))
        spin_until(executor, lambda: len(adapted) == 2 and len(ros) == 2)
        assert [value.text for value in adapted] == ['first', 'second']
        assert [message.data for message in ros] == ['first', 'second']
        assert 'source_timestamp' in infos[0]
        assert (adapted[0] is sent) == broker
        assert adapted[1].converted
        assert counted.to_ros_calls == 1
        assert counted.from_ros_calls == (1 if broker else 2)
    finally:
        executor.shutdown()
        node.destroy_node()


def test_broker_converts_once_and_only_for_other_readers(broker_context):
    context, broker = broker_context
    node = rclpy.create_node('adapted_readers', context=context)
    executor = SingleThreadedExecutor(context=context)
    counted, other = Counted(), Counted()
    same, ros, different = [], [], []
    try:
        publisher = node.create_publisher(counted.adapter(), 'adapted_readers', 10)
        for _ in range(2):
            node.create_subscription(counted.adapter(), 'adapted_readers', same.append, 10)
        executor.add_node(node)
        wait_for_subscriptions(executor, publisher, 2)
        publisher.publish(Text('alone'))
        spin_until(executor, lambda: len(same) == 2)
        assert counted.to_ros_calls == (0 if broker else 1)
        assert counted.from_ros_calls == (0 if broker else 2)
        if broker:
            assert same[0] is same[1]
        node.create_subscription(String, 'adapted_readers', ros.append, 10)
        node.create_subscription(TypeAdapter(Text, String, other.to_ros, other.from_ros),
                                 'adapted_readers', different.append, 10)
        wait_for_subscriptions(executor, publisher, 4)
        publisher.publish(Text('shared'))
        spin_until(executor, lambda: len(same) == 4 and ros and different)
        assert ros[0].data == 'shared'
        assert different[0].text == 'shared' and different[0].converted
        assert counted.to_ros_calls == (1 if broker else 2)
        assert other.to_ros_calls == 0 and other.from_ros_calls == 1
    finally:
        executor.shutdown()
        node.destroy_node()


def test_conversion_errors(broker_context):
    context, broker = broker_context
    node = rclpy.create_node('adapted_errors', context=context)

    def fail(value):
        raise ValueError('to_ros failed')

    try:
        publisher = node.create_publisher(
            TypeAdapter(Text, String, fail, Text), 'adapted_errors', 10)
        wrong = node.create_publisher(
            TypeAdapter(Text, String, lambda value: Text(value.text), Text), 'adapted_wrong', 10)
        if not broker:
            with pytest.raises(ValueError, match='to_ros failed'):
                publisher.publish(Text('x'))
            with pytest.raises(TypeError, match='to_ros returned'):
                wrong.publish(Text('x'))
        with pytest.raises(TypeError):
            publisher.publish(object())
        with pytest.raises(ValueError, match='Raw'):
            node.create_subscription(Counted().adapter(), 'adapted_errors', print, 10, raw=True)
    finally:
        node.destroy_node()


def test_remote_reader_converts_on_the_publishing_thread(broker_context, tmp_path):
    context, broker = broker_context
    node = rclpy.create_node('adapted_remote', context=context)
    executor = SingleThreadedExecutor(context=context)
    counted = Counted()
    script = tmp_path / 'reader.py'
    script.write_text('\n'.join([
        'import rclpy, sys',
        'from std_msgs.msg import String',
        'rclpy.init()',
        "node = rclpy.create_node('adapted_remote_reader')",
        "node.create_subscription(String, 'adapted_remote', lambda m: (print(m.data, flush=True),"
        ' sys.exit(0)), 10)',
        'rclpy.spin(node)',
    ]))
    reader = subprocess.Popen([sys.executable, str(script)], stdout=subprocess.PIPE, text=True)
    try:
        publisher = node.create_publisher(counted.adapter(), 'adapted_remote', 10)
        executor.add_node(node)
        wait_for_subscriptions(executor, publisher, 1)
        deadline = time.monotonic() + 10
        while reader.poll() is None:
            assert time.monotonic() < deadline
            publisher.publish(Text('remote'))
            time.sleep(0.1)
        assert reader.stdout.read().strip().endswith('remote')
        assert counted.to_ros_threads == {threading.get_ident()}
    finally:
        if reader.poll() is None:
            reader.kill()
        reader.wait()
        reader.stdout.close()
        executor.shutdown()
        node.destroy_node()


def test_adapted_object_outlives_the_publisher(broker_context):
    context, broker = broker_context
    node = rclpy.create_node('adapted_lifetime', context=context)
    executor = SingleThreadedExecutor(context=context)
    counted = Counted()
    received = []
    try:
        publisher = node.create_publisher(counted.adapter(), 'adapted_lifetime', 10)
        node.create_subscription(counted.adapter(), 'adapted_lifetime', received.append, 10)
        executor.add_node(node)
        wait_for_subscriptions(executor, publisher, 1)
        payload = numpy.arange(1 << 20, dtype=numpy.uint8)
        text = Text('big')
        text.payload = payload
        reference = weakref.ref(text)
        publisher.publish(text)
        del text
        node.destroy_publisher(publisher)
        spin_until(executor, lambda: received)
        assert received[0].text == 'big'
        if broker:
            assert received[0].payload is payload
        received.clear()
        gc.collect()
        assert reference() is None
    finally:
        executor.shutdown()
        node.destroy_node()
