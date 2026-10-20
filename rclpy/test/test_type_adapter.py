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

import time
from unittest.mock import Mock

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
    assert node.create_publisher.call_args.args == (String, 'adapted', 1)
    assert isinstance(publisher.ros_publisher.publish.call_args.args[0], String)
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
        with pytest.raises(TypeError):
            publisher.ros_publisher.publish(value)
        publisher.publish(value)
        wire_publisher.publish(Float64MultiArray(data=value))
        while len(received) < 2 or len(wire_received) < 2:
            assert time.monotonic() < deadline
            executor.spin_once(timeout_sec=0.01)
        assert calls == {'to_ros': 1, 'from_ros': 2}
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
