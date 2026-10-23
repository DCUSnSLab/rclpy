# Python type adapters

`rclpy.type_adapter.TypeAdapter` is the Python counterpart of `rclcpp::TypeAdapter`.
It binds a custom Python type to a ROS message type with two conversions,
`to_ros(custom)` and `from_ros(message)`. Pass the adapter, or a custom type that
has a registered adapter, where a message type is expected:

```python
import numpy
from std_msgs.msg import Float64MultiArray
from rclpy.type_adapter import TypeAdapter, register_type_adapter

adapter = TypeAdapter(
    numpy.ndarray, Float64MultiArray,
    lambda value: Float64MultiArray(data=value),
    lambda message: numpy.asarray(message.data))
register_type_adapter(adapter)
publisher = node.create_publisher(numpy.ndarray, 'samples', 10)
subscription = node.create_subscription(numpy.ndarray, 'samples', consume, 10)
publisher.publish(numpy.arange(1024, dtype=numpy.float64))
```

`register_type_adapter` makes an adapter the default for its custom type; a
conflicting registration raises `ValueError`. `node.create_publisher(adapter, ...)`
uses an adapter without registering it.

The topic, its type hash, discovery, QoS, content filtering and serialization are
those of the ROS message type. `Publisher.msg_type` and `Subscription.msg_type` are
the ROS type and `type_adapter` is the adapter. An adapted publisher publishes both
custom objects and ROS messages. An adapted subscription callback receives custom
objects and may be a coroutine and take message information as its second argument.
Raw subscriptions cannot use an adapter.

## Delivery

| Reader | What it receives from an adapted Python publisher |
| --- | --- |
| Subscription with an equal adapter, same rcl context, broker on | The published object itself. Neither conversion runs |
| Any other subscription in the same context, broker on | The ROS message from `to_ros`, converted once for all of them by the first reader that takes it. Python readers with an adapter call `from_ros` on it |
| Readers in other processes, or broker off | `to_ros` runs once while publishing; readers deserialize the message |

The broker is on with `RCL_INTRA_PROCESS=1` and `rmw_zenoh_cpp`. Adapters are
equal when their four fields are, so subscriptions created with the adapter the
publisher used, or with the registered one, receive the object. C++ subscriptions,
including `rclcpp::TypeAdapter` ones in a mixed component container, receive the ROS
message. A Python subscription receives the ROS message of an adapted C++ publisher
and calls `from_ros` on it.

The object is shared, not copied: the publisher must not modify it after
`publish`, and subscriptions receiving the same object must not modify it either.
Arrays inside it can be made read-only (`array.flags.writeable = False`) to have
NumPy enforce this.

`to_ros` runs on the publishing thread when a reader in another process exists, and
an exception it raises propagates from `publish`. When it runs later for a reader in
the same process, possibly on a thread of a C++ executor, an exception is reported
through `sys.unraisablehook` and that reader receives nothing. `to_ros` must return
an instance of the ROS message type; otherwise it fails with `TypeError`.

With `RCLPY_MESSAGE_ARRAYS=numpy`, the message given to `from_ros` exposes numeric
arrays as read-only NumPy views of the received C message, so `from_ros` can wrap
them without copying.

## Factory functions

`rclpy.type_adapter.create_publisher(node, custom_type, ...)` and
`create_subscription(node, custom_type, ...)` select a registered or explicit adapter
for a custom type and call the node methods. The publisher they return,
`AdaptedPublisher`, publishes custom objects only; its `ros_publisher` is the node's
publisher.
