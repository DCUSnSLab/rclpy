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

import array
import gc
import os
import weakref

import numpy
import pytest

from rclpy.serialization import deserialize_message
from rclpy.serialization import serialize_message
from std_msgs.msg import Float64MultiArray
from std_msgs.msg import UInt8MultiArray


@pytest.mark.parametrize('message_type,dtype', [
    (UInt8MultiArray, numpy.uint8), (Float64MultiArray, numpy.float64),
])
@pytest.mark.parametrize('size', [0, 50, 100000])
def test_owned_deserialized_arrays(message_type, dtype, size):
    original = message_type(data=numpy.arange(size, dtype=dtype))
    wire = serialize_message(original)
    message = deserialize_message(wire, message_type)
    data = message.data
    numpy.testing.assert_array_equal(data, original.data)
    if (os.getenv('RCLPY_MESSAGE_ARRAYS') != 'numpy' or
            getattr(message_type.__class__, '_CONVERT_TO_PY_WITH_OWNER', None) is None):
        assert isinstance(data, array.array)
        return
    assert isinstance(data, numpy.ndarray)
    assert not data.flags.writeable
    assert not data.flags.owndata
    with pytest.raises(ValueError):
        data.setflags(write=True)
    assert message == original
    assert deserialize_message(serialize_message(message), message_type) == original
    reference = weakref.ref(data)
    view = data[::2]
    del data, message
    gc.collect()
    for _ in range(5):
        deserialize_message(wire, message_type)
    numpy.testing.assert_array_equal(view, numpy.asarray(original.data)[::2])
    assert reference() is not None
    del view
    gc.collect()
    assert reference() is None
