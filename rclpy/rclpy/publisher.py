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

from typing import Generic, Optional, TypeVar, Union

from rclpy.callback_groups import CallbackGroup
from rclpy.duration import Duration
from rclpy.event_handler import EventHandler
from rclpy.event_handler import PublisherEventCallbacks
from rclpy.impl.implementation_singleton import rclpy_implementation as _rclpy
from rclpy.qos import QoSProfile
from rclpy.type_adapter import _broker_key
from rclpy.type_adapter import TypeAdapter

MsgType = TypeVar('MsgType')


class LoanedMessage(Generic[MsgType]):
    """
    A message borrowed from a publisher with :meth:`Publisher.borrow_loaned_message`.

    Publish it with :meth:`Publisher.publish`. Leaving a ``with`` block returns it unpublished.
    """

    def __init__(self, msg: MsgType, loan=None) -> None:
        self._msg: Optional[MsgType] = msg
        self._loan = loan

    @property
    def msg(self) -> MsgType:
        """Get the message to fill; its numeric arrays are writable views of the loan."""
        if self._msg is None:
            raise RuntimeError('the loaned message was already published or returned')
        return self._msg

    def __enter__(self) -> 'LoanedMessage[MsgType]':
        return self

    def __exit__(self, *_) -> None:
        self._release()

    def _release(self) -> None:
        self._msg = None
        self._loan = None


class Publisher:

    _type_adapter: Optional[TypeAdapter] = None

    def __init__(
        self,
        publisher_impl: _rclpy.Publisher,
        msg_type: MsgType,
        topic: str,
        qos_profile: QoSProfile,
        event_callbacks: PublisherEventCallbacks,
        callback_group: CallbackGroup,
    ) -> None:
        """
        Create a container for a ROS publisher.

        .. warning:: Users should not create a publisher with this constructor, instead they should
           call :meth:`.Node.create_publisher`.

        A publisher is used as a primary means of communication in a ROS system by publishing
        messages on a ROS topic.

        :param publisher_impl: Publisher wrapping the underlying ``rcl_publisher_t`` object.
        :param msg_type: The type of ROS messages the publisher will publish.
        :param topic: The name of the topic the publisher will publish to.
        :param qos_profile: The quality of service profile to apply to the publisher.
        """
        self.__publisher = publisher_impl
        self.msg_type = msg_type
        self.topic = topic
        self.qos_profile = qos_profile

        self.event_handlers: EventHandler = event_callbacks.create_event_handlers(
            callback_group, publisher_impl, topic)

    def publish(self, msg: Union[MsgType, bytes, LoanedMessage[MsgType]]) -> None:
        """
        Send a message to the topic for the publisher.

        :param msg: The ROS message to publish. A :class:`LoanedMessage` is published without
          copying it, unless NumPy views of it are still referenced outside of its message; then
          a copy is published. Either way its message can no longer be used.
          A publisher with a :attr:`type_adapter` also publishes instances of its custom type.
          Subscriptions with the same adapter in this process receive the object itself through
          the intra-process broker, so it must not be modified after it is published.
        :raises: TypeError if the type of the passed message isn't an instance
          of the provided type when the publisher was constructed.
        """
        with self.handle:
            if isinstance(msg, self.msg_type):
                self.__publisher.publish(msg)
            elif self._type_adapter is not None and \
                    isinstance(msg, self._type_adapter.custom_type):
                adapter = self._type_adapter
                self.__publisher.publish_adapted(
                    msg, _broker_key(adapter), adapter.to_ros, self.msg_type)
            elif isinstance(msg, bytes):
                self.__publisher.publish_raw(msg)
            elif isinstance(msg, LoanedMessage):
                self._publish_loaned_message(msg)
            else:
                raise TypeError('Expected {}, got {}'.format(self.msg_type, type(msg)))

    def _publish_loaned_message(self, loaned: LoanedMessage[MsgType]) -> None:
        msg = loaned.msg
        if not isinstance(msg, self.msg_type):
            raise TypeError('Expected {}, got {}'.format(self.msg_type, type(msg)))
        loan = loaned._loan
        loaned._release()
        if loan is None:
            self.__publisher.publish(msg)
        else:
            self.__publisher.publish_loaned_message(loan, msg)

    @property
    def can_loan_messages(self) -> bool:
        """
        Whether :meth:`borrow_loaned_message` lends middleware memory.

        The middleware lends only types of fixed size, such as messages without strings or
        unbounded arrays.
        """
        with self.handle:
            return self.__publisher.can_loan_messages()

    def borrow_loaned_message(self) -> LoanedMessage[MsgType]:
        """
        Borrow a message to fill in place and publish with :meth:`publish`.

        When the middleware lends messages to this publisher, the message is its memory and the
        numeric arrays of the message are writable NumPy views of it, so publishing it copies
        nothing. What a lent message holds before it is filled depends on the middleware:
        rmw_zenoh_cpp gives the default values of the type, others may give an earlier message,
        so set every field. Otherwise the message is an ordinary new message.
        """
        with self.handle:
            if not self.__publisher.can_loan_messages():
                return LoanedMessage(self.msg_type())
            loan, msg = self.__publisher.borrow_loaned_message(self.msg_type)
            return LoanedMessage(msg, loan)

    @property
    def type_adapter(self) -> Optional[TypeAdapter]:
        """Get the type adapter the publisher was created with, or ``None``."""
        return self._type_adapter

    def get_subscription_count(self) -> int:
        """Get the amount of subscribers that this publisher has."""
        with self.handle:
            return self.__publisher.get_subscription_count()

    @property
    def topic_name(self) -> str:
        with self.handle:
            return self.__publisher.get_topic_name()

    @property
    def handle(self):
        return self.__publisher

    @property
    def logger_name(self) -> str:
        """Get the name of the logger associated with the node of the publisher."""
        with self.handle:
            return self.__publisher.get_logger_name()

    def destroy(self):
        """
        Destroy a container for a ROS publisher.

        .. warning:: Users should not destroy a publisher with this method, instead they should
           call :meth:`.Node.destroy_publisher`.
        """
        for handler in self.event_handlers:
            handler.destroy()
        self.__publisher.destroy_when_not_in_use()

    def assert_liveliness(self) -> None:
        """
        Manually assert that this Publisher is alive.

        If the QoS Liveliness policy is set to MANUAL_BY_TOPIC, the
        application must call this at least as often as ``QoSProfile.liveliness_lease_duration``.
        """
        with self.handle:
            _rclpy.rclpy_assert_liveliness(self.handle)

    def wait_for_all_acked(self, timeout: Duration = Duration(seconds=-1)) -> bool:
        """
        Wait until all published message data is acknowledged or until the timeout elapses.

        If the timeout is negative then this function will block indefinitely until all published
        message data is acknowledged.
        If the timeout is 0 then it will check if all published message has been acknowledged
        without waiting.
        If the timeout is greater than 0 then it will return after that period of time has elapsed
        or all published message data is acknowledged.
        This function only waits for acknowledgments if the publisher's QOS profile is RELIABLE.
        Otherwise this function will immediately return true.

        :param timeout: the duration to wait for all published message data to be acknowledged.
        :returns: true if all published message data is acknowledged before the timeout, otherwise
            false.
        """
        with self.handle:
            return self.__publisher.wait_for_all_acked(timeout._duration_handle)
