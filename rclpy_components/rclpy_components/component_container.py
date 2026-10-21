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

import rclpy
from rclpy.executors import ExternalShutdownException
from rclpy.executors import MultiThreadedExecutor
from rclpy.executors import SingleThreadedExecutor

from .component_manager import ComponentManager


def main(args=None, executor_type=SingleThreadedExecutor):
    rclpy.init(args=args)
    executor = executor_type()
    component_manager = ComponentManager(executor)
    executor.add_node(component_manager)
    try:
        executor.spin()
    except (KeyboardInterrupt, ExternalShutdownException):
        pass
    component_manager.destroy_node()
    executor.shutdown()
    rclpy.try_shutdown()


def main_mt(args=None):
    main(args, MultiThreadedExecutor)
