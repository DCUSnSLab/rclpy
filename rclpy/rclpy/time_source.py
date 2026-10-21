# Copyright 2018 Open Source Robotics Foundation, Inc.
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

import threading
from typing import Callable, List, Optional, Set, TYPE_CHECKING
import weakref

from rcl_interfaces.msg import SetParametersResult
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.clock import ROSClock
from rclpy.clock_type import ClockType
from rclpy.impl.implementation_singleton import rclpy_implementation as _rclpy
from rclpy.parameter import Parameter
from rclpy.qos import QoSProfile
from rclpy.qos import ReliabilityPolicy
from rclpy.time import Time
import rosgraph_msgs.msg

if TYPE_CHECKING:
    from rclpy.node import Node
    from rclpy.subscription import Subscription

CLOCK_TOPIC = '/clock'
USE_SIM_TIME_NAME = 'use_sim_time'
CLOCK_QOS = QoSProfile(depth=1, reliability=ReliabilityPolicy.BEST_EFFORT)


class _ClockThread:
    """Take /clock on a thread of its own, so that callbacks do not delay ROS time."""

    def __init__(
        self, node: 'Node', on_clock: Callable[[rosgraph_msgs.msg.Clock], None]
    ) -> None:
        self._context = node.context
        with node.handle:
            self._subscription = _rclpy.Subscription(
                node.handle, rosgraph_msgs.msg.Clock, CLOCK_TOPIC,
                CLOCK_QOS.get_c_qos_profile(), None)
        with self._context.handle:
            self._stop = _rclpy.GuardCondition(self._context.handle)
        self._on_clock = on_clock
        self._thread = threading.Thread(target=self._run, name='rclpy_clock', daemon=True)
        self._thread.start()
        # The subscription must be gone before the middleware shuts down.
        self._context.on_shutdown(self.stop)

    def _run(self) -> None:
        with self._subscription, self._stop, self._context.handle:
            wait_set = _rclpy.WaitSet(1, 1, 0, 0, 0, 0, self._context.handle)
            while True:
                wait_set.clear_entities()
                wait_set.add_subscription(self._subscription)
                wait_set.add_guard_condition(self._stop)
                wait_set.wait(-1)
                if wait_set.get_ready_entities('guard_condition'):
                    return
                taken = self._subscription.take_message(rosgraph_msgs.msg.Clock, False)
                if taken is not None:
                    self._on_clock(taken[0])

    def stop(self) -> None:
        if not self._thread.is_alive():
            return
        self._stop.trigger_guard_condition()
        self._thread.join()
        self._subscription.destroy_when_not_in_use()
        self._stop.destroy_when_not_in_use()


class TimeSource:

    def __init__(self, *, node: Optional['Node'] = None, use_clock_thread: bool = False):
        """
        Create a TimeSource.

        :param node: Node to attach, see :meth:`attach_node`.
        :param use_clock_thread: Take "/clock" on a dedicated thread instead of in the executor
            of the node, so that ROS time advances while callbacks run.
        """
        self._clock_sub: Optional['Subscription'] = None
        self._clock_thread: Optional[_ClockThread] = None
        self._use_clock_thread = use_clock_thread
        self._node_weak_ref: Optional[weakref.ReferenceType['Node']] = None
        self._associated_clocks: Set[ROSClock] = set()
        # Zero time is a special value that means time is uninitialized
        self._last_time_set = Time(clock_type=ClockType.ROS_TIME)
        self._ros_time_is_active = False
        if node is not None:
            self.attach_node(node)

    @property
    def ros_time_is_active(self) -> bool:
        return self._ros_time_is_active

    @ros_time_is_active.setter
    def ros_time_is_active(self, enabled: bool) -> None:
        if self._ros_time_is_active == enabled:
            return
        self._ros_time_is_active = enabled
        for clock in self._associated_clocks:
            clock._set_ros_time_is_active(enabled)
        if enabled:
            self._subscribe_to_clock_topic()
        else:
            self._stop_clock_thread()
            if self._clock_sub is not None:
                node = self._get_node()
                if node is not None:
                    node.destroy_subscription(self._clock_sub)
                    self._clock_sub = None

    def _subscribe_to_clock_topic(self) -> None:
        if self._clock_sub is None and self._clock_thread is None:
            node = self._get_node()
            if node is None:
                return
            if self._use_clock_thread:
                self._clock_thread = _ClockThread(node, self.clock_callback)
                return
            # A group of its own lets a multithreaded executor update the time while a
            # callback of the default group runs (ros2/rclpy#792).
            self._clock_sub = node.create_subscription(
                rosgraph_msgs.msg.Clock,
                CLOCK_TOPIC,
                self.clock_callback,
                CLOCK_QOS,
                callback_group=MutuallyExclusiveCallbackGroup()
            )

    def _stop_clock_thread(self) -> None:
        if self._clock_thread is not None:
            self._clock_thread.stop()
            self._clock_thread = None

    def attach_node(self, node: 'Node') -> None:
        from rclpy.node import Node
        if not isinstance(node, Node):
            raise TypeError('Node must be of type rclpy.node.Node')
        # Remove an existing node.
        if self._node_weak_ref is not None:
            self.detach_node()
        self._node_weak_ref = weakref.ref(node)

        if not node.has_parameter(USE_SIM_TIME_NAME):
            node.declare_parameter(USE_SIM_TIME_NAME, False)

        use_sim_time_param = node.get_parameter(USE_SIM_TIME_NAME)
        if use_sim_time_param.type_ != Parameter.Type.NOT_SET:
            if use_sim_time_param.type_ == Parameter.Type.BOOL:
                self.ros_time_is_active = use_sim_time_param.value
            else:
                node.get_logger().error(
                    "Invalid type for parameter '{}' {!r} should be bool"
                    .format(USE_SIM_TIME_NAME, use_sim_time_param.type_))
        else:
            node.get_logger().debug(
                "'{}' parameter not set, using wall time by default"
                .format(USE_SIM_TIME_NAME))

        node.add_on_set_parameters_callback(self._on_parameter_event)

    def detach_node(self) -> None:
        self._stop_clock_thread()
        # Remove the subscription to the clock topic.
        if self._clock_sub is not None:
            node = self._get_node()
            if node is None:
                raise RuntimeError('Unable to destroy previously created clock subscription')
            node.destroy_subscription(self._clock_sub)
        self._clock_sub = None
        self._node_weak_ref = None

    def attach_clock(self, clock: ROSClock) -> None:
        if not isinstance(clock, ROSClock):
            raise ValueError('Only clocks with type ROS_TIME can be attached.')

        clock.set_ros_time_override(self._last_time_set)
        clock._set_ros_time_is_active(self.ros_time_is_active)
        self._associated_clocks.add(clock)

    def clock_callback(self, msg: rosgraph_msgs.msg.Clock) -> None:
        # Cache the last message in case a new clock is attached.
        time_from_msg = Time.from_msg(msg.clock)
        self._last_time_set = time_from_msg
        for clock in self._associated_clocks:
            clock.set_ros_time_override(time_from_msg)

    def _on_parameter_event(self, parameter_list: List[Parameter]) -> SetParametersResult:
        successful = True
        reason = ''

        for parameter in parameter_list:
            if parameter.name == USE_SIM_TIME_NAME:
                if parameter.type_ == Parameter.Type.BOOL:
                    self.ros_time_is_active = parameter.value
                else:
                    successful = False
                    reason = '{} parameter set to something besides a bool'.format(
                        USE_SIM_TIME_NAME)

                    node = self._get_node()
                    if node:
                        node.get_logger().error(reason)
                break

        return SetParametersResult(successful=successful, reason=reason)

    def _get_node(self) -> Optional['Node']:
        if self._node_weak_ref is not None:
            return self._node_weak_ref()
        return None
