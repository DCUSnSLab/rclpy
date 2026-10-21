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

from rclpy.node import Node
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
