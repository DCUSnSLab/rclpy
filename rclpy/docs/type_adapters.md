# Python type adapters and the WP4 boundary

The prototype lives in `rclpy.type_adapter`. A `TypeAdapter` binds a custom Python
class to a generated ROS wire class, `to_ros(custom)`, and `from_ros(message)`.
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

The prototype sends through the existing RMW path, including between two nodes in
one process. It makes no promise of avoiding conversion for local delivery. Its
purpose is to exercise registration, wire interoperability, receive-view ownership,
and callbacks before WP4 supplies the local delivery mechanism. Without an adapter,
ordinary `Publisher.publish(numpy_array)` raises `TypeError`; the prototype accepts
it and interoperates with ordinary ROS subscribers and publishers.

## Contract for WP4

Each endpoint must carry both the ROS type identity and the adapter identity.
Adapter identity includes a user-supplied semantic identifier/version, custom type,
and wire type/hash; Python class identity alone cannot describe dtype or units.
Compatible local endpoints enqueue the custom object, retaining a strong reference,
and do not call either conversion function. Local endpoints with different custom
representations pass through the common ROS representation once. Remote recipients
cause `to_ros` to run once per publication, shared across remote recipients and local
ROS consumers. Mixed local/remote fanout therefore uses both paths without duplicate
delivery. Whether a route crosses the boundary must come from WP4's matched-endpoint
routing, not from total subscription counts or process-ID heuristics.

Local queues must participate in executor wait/ready handling, callback groups,
QoS depth, shutdown, and tracing. A synchronous callback shortcut would violate
these rules. Discovery changes must atomically change route snapshots so that a
new remote reader cannot receive an unconverted sample. Transient-local history
must retain a wire representation or convert retained custom samples when a remote
reader joins, with explicit memory limits and error handling. Serialized/raw
consumers always select the wire path.

A local payload is immutable while queued or in callbacks. Read-only NumPy flags
alone cannot freeze other writable aliases. WP4 must choose an explicit ownership
transfer API or a publisher-side copy/freeze policy; mutable ordinary publish keeps
its current snapshot semantics. External-process reception may use
`RCLPY_MESSAGE_ARRAYS=numpy`: `from_ros` can return an ndarray or a view whose base
retains the C message even after the wrapper message and subscription are gone.
Callback code that needs writable arrays makes an explicit copy. No C/C++ public
class layout or existing conversion symbol needs to change for this adapter API.

## Integration after the prototype

Node factories can accept an explicit `type_adapter=` keyword and return native
Publisher/Subscription objects after WP4 has endpoint metadata storage. Keep
`msg_type` as the ROS wire type for existing executor/type-support code and expose
`custom_type` separately. Resolve registration only when creating an endpoint;
never inspect the global registry on the hot path. Adaptation belongs at WP4's
wire conversion boundary, immediately before C conversion and immediately after
owned receive conversion, rather than inside the generated message classes.
