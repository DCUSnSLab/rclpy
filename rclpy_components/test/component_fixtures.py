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


class FooNode(Node):

    def __init__(self, **kwargs):
        super().__init__('test_foo', **kwargs)
        self.answer = self.declare_parameter('answer', 0).value


class Broken(Node):

    def __init__(self, **kwargs):
        raise ValueError('Oops I failed to construct')
