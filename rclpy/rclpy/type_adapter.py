# Copyright 2016 Open Source Robotics Foundation, Inc.
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

"""Experimental adapters for custom Python types at the ROS message boundary."""

from dataclasses import dataclass
from functools import wraps
import inspect
from typing import Any, Callable, Type


@dataclass(frozen=True)
class TypeAdapter:
    """Describe the wire type and the two boundary conversions for a Python type."""

    custom_type: Type
    ros_type: Type
    to_ros: Callable[[Any], Any]
    from_ros: Callable[[Any], Any]


_adapters = {}


def register_type_adapter(adapter: TypeAdapter) -> None:
    """Register one default wire representation for a custom Python type."""
    previous = _adapters.get(adapter.custom_type)
    if previous is not None and previous != adapter:
        raise ValueError('A different adapter is already registered for this type')
    _adapters[adapter.custom_type] = adapter


class AdaptedPublisher:
    """Publish a custom Python type through an ordinary ROS publisher."""

    def __init__(self, ros_publisher, adapter: TypeAdapter):
        """Retain the ROS publisher and the selected adapter."""
        self.ros_publisher = ros_publisher
        self.adapter = adapter

    def publish(self, value) -> None:
        """Convert once at the ROS boundary and publish the resulting message."""
        if not isinstance(value, self.adapter.custom_type):
            raise TypeError(f'Expected {self.adapter.custom_type}, got {type(value)}')
        self.ros_publisher.publish(self.adapter.to_ros(value))


def create_publisher(node, custom_type, topic, qos_profile, *, adapter=None, **kwargs):
    """Create an adapted publisher using an explicit or registered adapter."""
    selected = _select_adapter(custom_type, adapter)
    publisher = node.create_publisher(selected.ros_type, topic, qos_profile, **kwargs)
    return AdaptedPublisher(publisher, selected)


def create_subscription(
        node, custom_type, topic, callback, qos_profile, *, adapter=None, **kwargs):
    """Create a ROS subscription that converts before invoking its user callback."""
    selected = _select_adapter(custom_type, adapter)
    if kwargs.get('raw', False):
        raise ValueError('Raw subscriptions cannot use a type adapter')
    if inspect.iscoroutinefunction(callback):
        @wraps(callback)
        async def receive(message, *args):
            await callback(selected.from_ros(message), *args)
    else:
        @wraps(callback)
        def receive(message, *args):
            callback(selected.from_ros(message), *args)
    return node.create_subscription(selected.ros_type, topic, receive, qos_profile, **kwargs)


def _select_adapter(custom_type, adapter):
    selected = adapter if adapter is not None else _adapters.get(custom_type)
    if selected is None:
        raise TypeError(f'No adapter registered for {custom_type}')
    if selected.custom_type is not custom_type:
        raise TypeError('Adapter custom_type does not match the requested type')
    return selected
