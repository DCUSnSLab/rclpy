# Copyright 2026 Open Source Robotics Foundation, Inc.
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
import time

from geometry_msgs.msg import PoseWithCovariance
import numpy
import pytest
import rclpy
import rclpy.executors
from rclpy.publisher import LoanedMessage
from std_msgs.msg import String
from test_msgs.msg import BasicTypes

# Subscriptions read this when they are created.
os.environ['ROS_DISABLE_LOANED_MESSAGES'] = '0'


@pytest.fixture
def node():
    context = rclpy.context.Context()
    rclpy.init(context=context)
    node = rclpy.create_node('test_loaned_messages', context=context)
    node.test_executor = rclpy.executors.SingleThreadedExecutor(context=context)
    node.test_executor.add_node(node)
    yield node
    node.test_executor.shutdown()
    node.destroy_node()
    rclpy.shutdown(context=context)


def connect(node, msg_type, topic):
    received = []
    sub = node.create_subscription(msg_type, topic, received.append, 10)
    pub = node.create_publisher(msg_type, topic, 10)
    deadline = time.monotonic() + 5
    while pub.get_subscription_count() < 1 or sub.get_publisher_count() < 1:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    return pub, sub, received


def spin_until(node, received, count):
    deadline = time.monotonic() + 5
    while len(received) < count and time.monotonic() < deadline:
        node.test_executor.spin_once(timeout_sec=0.05)
    assert len(received) == count


def has_views(msg_type):
    return getattr(type(msg_type), '_CONVERT_TO_PY_WITH_OWNER', None) is not None


def fill(msg, value):
    msg.pose.position.x = float(value)
    msg.pose.orientation.w = -float(value)
    msg.covariance[:] = numpy.arange(36) + value


def check(msg, value):
    assert msg.pose.position.x == value
    assert msg.pose.orientation.w == -value
    numpy.testing.assert_array_equal(msg.covariance, numpy.arange(36) + value)


def test_publish_loan(node):
    pub, sub, received = connect(node, PoseWithCovariance, 'loan')
    loaned = pub.borrow_loaned_message()
    msg = loaned.msg
    if pub.can_loan_messages and has_views(PoseWithCovariance):
        assert msg.covariance.flags.writeable
        assert not msg.covariance.flags.owndata
    fill(msg, 3.0)
    pub.publish(loaned)
    spin_until(node, received, 1)
    check(received[0], 3.0)
    with pytest.raises(RuntimeError):
        loaned.msg
    with pytest.raises(RuntimeError):
        pub.publish(loaned)


def test_published_message_no_longer_reaches_the_loan(node):
    pub, sub, received = connect(node, PoseWithCovariance, 'detach')
    loaned = pub.borrow_loaned_message()
    msg = loaned.msg
    fill(msg, 1.0)
    if not pub.can_loan_messages:
        pytest.skip('the middleware does not lend this type')
    assert pub.handle.publish_loaned_message(loaned._loan, msg)
    if has_views(PoseWithCovariance):
        with pytest.raises(AttributeError):
            msg.covariance
    spin_until(node, received, 1)
    check(received[0], 1.0)


def test_referenced_view_publishes_a_copy(node):
    pub, sub, received = connect(node, PoseWithCovariance, 'escape')
    if not pub.can_loan_messages or not has_views(PoseWithCovariance):
        pytest.skip('the middleware does not lend this type, or it has no views')
    loaned = pub.borrow_loaned_message()
    fill(loaned.msg, 2.0)
    view = loaned.msg.covariance[4:]
    assert not pub.handle.publish_loaned_message(loaned._loan, loaned.msg)
    view[:] = -1.0
    spin_until(node, received, 1)
    check(received[0], 2.0)
    del view
    gc.collect()
    for value in range(20):
        loaned = pub.borrow_loaned_message()
        fill(loaned.msg, value)
        pub.publish(loaned)
        spin_until(node, received, value + 2)
        check(received[-1], value)


def test_unpublished_loans_are_returned(node):
    pub, sub, received = connect(node, PoseWithCovariance, 'returned')
    for value in range(200):
        with pub.borrow_loaned_message() as loaned:
            fill(loaned.msg, value)
        with pytest.raises(RuntimeError):
            loaned.msg
    loaned = pub.borrow_loaned_message()
    fill(loaned.msg, 5.0)
    pub.publish(loaned)
    spin_until(node, received, 1)
    check(received[0], 5.0)


def test_types_without_arrays(node):
    pub, sub, received = connect(node, BasicTypes, 'basic')
    loaned = pub.borrow_loaned_message()
    loaned.msg.int64_value = -5
    loaned.msg.float64_value = 0.25
    loaned.msg.bool_value = True
    pub.publish(loaned)
    spin_until(node, received, 1)
    assert received[0] == BasicTypes(int64_value=-5, float64_value=0.25, bool_value=True)


def test_types_the_middleware_does_not_lend(node):
    pub, sub, received = connect(node, String, 'string')
    assert not pub.can_loan_messages
    assert not sub.can_loan_messages
    loaned = pub.borrow_loaned_message()
    loaned.msg.data = 'not lent'
    pub.publish(loaned)
    spin_until(node, received, 1)
    assert received[0].data == 'not lent'


def test_publish_checks_the_type(node):
    pub = node.create_publisher(PoseWithCovariance, 'type', 10)
    with pytest.raises(TypeError):
        pub.publish(LoanedMessage(String()))


def test_received_loans_outlive_the_callback(node):
    pub, sub, received = connect(node, PoseWithCovariance, 'held')
    for value in range(12):
        loaned = pub.borrow_loaned_message()
        fill(loaned.msg, value)
        pub.publish(loaned)
        spin_until(node, received, value + 1)
    for value, msg in enumerate(received):
        check(msg, value)
        if os.environ.get('RCLPY_MESSAGE_ARRAYS') == 'numpy':
            assert not msg.covariance.flags.writeable


def test_loans_outliving_their_publisher(recwarn):
    context = rclpy.context.Context()
    rclpy.init(context=context)
    node = rclpy.create_node('test_loans_outliving', context=context)
    pub = node.create_publisher(PoseWithCovariance, 'outliving', 10)
    loans = [pub.borrow_loaned_message() for _ in range(2)]
    fill(loans[0].msg, 1.0)
    node.destroy_node()
    fill(loans[1].msg, 2.0)
    rclpy.shutdown(context=context)
    del loans
    gc.collect()
    assert not [w for w in recwarn if 'Failed to fini' in str(w.message)]
