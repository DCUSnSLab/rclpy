# Python type adapters

`rclpy.type_adapter` is experimental. A `TypeAdapter` binds a custom Python class
to a generated ROS wire class, `to_ros(custom)`, and `from_ros(message)`.
Registering an adapter supplies the default representation for that Python class;
an endpoint can select an explicit adapter to use another representation. Existing
endpoints retain their selected adapter when later registrations are attempted.
Conflicting default registrations fail instead of changing running endpoints.

```python
import numpy
from std_msgs.msg import Float64MultiArray
from rclpy.type_adapter import (
    TypeAdapter, create_publisher, create_subscription, register_type_adapter)

adapter = TypeAdapter(
    numpy.ndarray, Float64MultiArray,
    lambda value: Float64MultiArray(data=value),
    lambda message: numpy.asarray(message.data))
register_type_adapter(adapter)
publisher = create_publisher(node, numpy.ndarray, 'samples', 10)
subscription = create_subscription(node, numpy.ndarray, 'samples', consume, 10)
publisher.publish(numpy.arange(1024, dtype=numpy.float64))
node.destroy_subscription(subscription)
node.destroy_publisher(publisher.ros_publisher)
```

The ROS wire type, type hash, discovery, QoS, content filtering, tracing, and
serialization remain those of `Float64MultiArray`. Array dtype, dimensions,
strides, units, and any layout metadata belong to the adapter's contract. This
example accepts native float64 vectors; the generated setter checks that contract.
The user callback may be synchronous or asynchronous and may receive message
information as its second argument. Conversion exceptions propagate through the
normal publish/callback error path. Raw subscriptions are rejected because their
callbacks receive serialized bytes rather than the adapter's ROS message type.
Without an adapter, ordinary `Publisher.publish(numpy_array)` raises `TypeError`.

## Delivery

`AdaptedPublisher.publish` calls `to_ros` once and publishes the ROS message with
the ordinary `Publisher`. The adapted subscription calls `from_ros` on every
received message before the user callback. Both conversions always run, including
between two nodes in one process.

The message between them takes the ordinary rclpy path:

| Route | What happens to the ROS message |
| --- | --- |
| rcl broker (`RCL_INTRA_PROCESS=1`, `rmw_zenoh_cpp`, same rcl context) | The C message built from the Python message is handed to the broker without serialization. An rclpy subscription takes that C message without a copy and converts it to a Python message; a C++ subscription receives one conversion to its C++ type |
| RMW | Serialized and delivered as for any other publisher |

With `RCLPY_MESSAGE_ARRAYS=numpy`, the message given to `from_ros` exposes numeric
arrays as read-only NumPy views of the received C message. `from_ros` can return
such a view; its base keeps the C message alive after the wrapper message and the
subscription are gone. Callback code that needs writable arrays makes an explicit
copy.

Publication snapshots the Python value into a C message, so changing the custom
object after `publish` does not change what subscribers receive.

## Limits

- Custom Python objects are not shared between endpoints. The rcl broker can share
  an adapted object by key (`rcl_publish_ipc_adapted`), but `rclpy.type_adapter`
  does not use it.
- `Node.create_publisher` and `Node.create_subscription` take no adapter argument;
  use the factory functions in `rclpy.type_adapter`.
- Serialized and raw consumers always receive the wire representation.
