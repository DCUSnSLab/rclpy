# Copyright 2026 nevlife
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

import rclpy
from rclpy.node import Node
from rclpy.type_adapter import TypeAdapter
from std_msgs.msg import String


class FooNode(Node):

    def __init__(self, **kwargs):
        super().__init__('test_foo', **kwargs)
        self.answer = self.declare_parameter('answer', 0).value


class Broken(Node):

    def __init__(self, **kwargs):
        raise ValueError('Oops I failed to construct')


class Talker(Node):

    def __init__(self, **kwargs):
        super().__init__('py_talker', **kwargs)
        self.count = 0
        self.publisher = self.create_publisher(String, 'chatter', 10)
        self.create_timer(0.1, self.on_timer)

    def on_timer(self):
        self.count += 1
        self.publisher.publish(String(data=f'py {self.count}'))


class Relay(Node):
    """Republish every received chatter message on heard, prefixed with 'relay '."""

    def __init__(self, **kwargs):
        super().__init__('py_relay', **kwargs)
        self.publisher = self.create_publisher(String, 'heard', 10)
        self.create_subscription(String, 'chatter', self.on_chatter, 10)

    def on_chatter(self, msg):
        self.publisher.publish(String(data='relay ' + msg.data))


class OkProbe(Node):
    """Publish what rclpy.ok() reports for the default context, as worker loops use it."""

    def __init__(self, **kwargs):
        super().__init__('py_ok_probe', **kwargs)
        self.publisher = self.create_publisher(String, 'probe', 10)
        self.create_timer(0.1, self.on_timer)

    def on_timer(self):
        self.publisher.publish(String(data=f'ok {rclpy.ok()}'))


class FailingDestroy(Node):

    def __init__(self, **kwargs):
        super().__init__('py_failing_destroy', **kwargs)

    def destroy_node(self):
        super().destroy_node()
        raise RuntimeError('destroy_node failed')


class Text:

    def __init__(self, text, converted=False):
        self.text = text
        self.converted = converted


TEXT_ADAPTER = TypeAdapter(
    Text, String, lambda value: String(data=value.text),
    lambda message: Text(message.data, converted=True))


class AdaptedTalker(Node):
    """Publish Text objects on chatter through a type adapter."""

    def __init__(self, **kwargs):
        super().__init__('py_adapted_talker', **kwargs)
        self.count = 0
        self.publisher = self.create_publisher(TEXT_ADAPTER, 'chatter', 10)
        self.create_timer(0.1, self.on_timer)

    def on_timer(self):
        self.count += 1
        self.publisher.publish(Text(f'adapted {self.count}'))


class AdaptedListener(Node):
    """Report on heard each Text received on chatter and whether from_ros made it."""

    def __init__(self, **kwargs):
        super().__init__('py_adapted_listener', **kwargs)
        self.publisher = self.create_publisher(String, 'heard', 10)
        self.create_subscription(TEXT_ADAPTER, 'chatter', self.on_chatter, 10)

    def on_chatter(self, value):
        self.publisher.publish(String(data=f'listener {value.text} converted={value.converted}'))
