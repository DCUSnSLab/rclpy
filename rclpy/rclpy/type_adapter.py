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

"""Adapters between custom Python types and ROS messages, like ``rclcpp::TypeAdapter``."""

from dataclasses import dataclass
import itertools
import threading
from typing import Any, Callable, Dict, Optional, Type


@dataclass(frozen=True)
class TypeAdapter:
    """
    Describe the ROS message type of a custom Python type and the two conversions.

    Pass it, or a custom type it was registered for, as the message type of
    :meth:`.Node.create_publisher` and :meth:`.Node.create_subscription`.

    :param custom_type: The type publishers accept and subscription callbacks receive.
    :param ros_type: The ROS message type on the topic.
    :param to_ros: Returns an instance of ``ros_type`` for an instance of ``custom_type``.
    :param from_ros: Returns an instance of ``custom_type`` for an instance of ``ros_type``.
    """

    custom_type: Type
    ros_type: Type
    to_ros: Callable[[Any], Any]
    from_ros: Callable[[Any], Any]


_adapters: Dict[Type, TypeAdapter] = {}
_keys: Dict[TypeAdapter, str] = {}
_keys_lock = threading.Lock()
_key_numbers = itertools.count()


def register_type_adapter(adapter: TypeAdapter) -> None:
    """Make an adapter the one used for its custom type when only the type is given."""
    previous = _adapters.get(adapter.custom_type)
    if previous is not None and previous != adapter:
        raise ValueError('A different adapter is already registered for this type')
    _adapters[adapter.custom_type] = adapter


def resolve_type_adapter(msg_type) -> Optional[TypeAdapter]:
    """Return the adapter that ``msg_type`` names, or ``None`` for a ROS message type."""
    if isinstance(msg_type, TypeAdapter):
        return msg_type
    try:
        return _adapters.get(msg_type)
    except TypeError:
        return None


def _broker_key(adapter: TypeAdapter) -> str:
    # Equal adapters convert alike, so they share the key that lets the broker hand over objects.
    with _keys_lock:
        key = _keys.get(adapter)
        if key is None:
            key = _keys[adapter] = f'rclpy:{next(_key_numbers)}'
        return key


class AdaptedPublisher:
    """A publisher of :func:`create_publisher`; :meth:`.Node.create_publisher` replaces it."""

    def __init__(self, ros_publisher, adapter: TypeAdapter):
        """Retain the publisher and the selected adapter."""
        self.ros_publisher = ros_publisher
        self.adapter = adapter

    def publish(self, value) -> None:
        """Publish an instance of the adapter's custom type."""
        if not isinstance(value, self.adapter.custom_type):
            raise TypeError(f'Expected {self.adapter.custom_type}, got {type(value)}')
        self.ros_publisher.publish(value)


def create_publisher(node, custom_type, topic, qos_profile, *, adapter=None, **kwargs):
    """Create an adapted publisher using an explicit or registered adapter."""
    selected = _select_adapter(custom_type, adapter)
    return AdaptedPublisher(
        node.create_publisher(selected, topic, qos_profile, **kwargs), selected)


def create_subscription(
        node, custom_type, topic, callback, qos_profile, *, adapter=None, **kwargs):
    """Create a subscription whose callback receives the adapter's custom type."""
    selected = _select_adapter(custom_type, adapter)
    if kwargs.get('raw', False):
        raise ValueError('Raw subscriptions cannot use a type adapter')
    return node.create_subscription(selected, topic, callback, qos_profile, **kwargs)


def _select_adapter(custom_type, adapter):
    selected = adapter if adapter is not None else _adapters.get(custom_type)
    if selected is None:
        raise TypeError(f'No adapter registered for {custom_type}')
    if selected.custom_type is not custom_type:
        raise TypeError('Adapter custom_type does not match the requested type')
    return selected
