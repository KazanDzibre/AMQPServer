from pika import spec
import struct
from pika import exceptions
from pika.compat import byte
from pika.connection import Parameters

HOSTNAME = 'localhost'
MAX_BYTES = 4096
CHANNEL_MAX = 2047
FRAME_MAX = 131072
HEARTBEAT = 60
MAJOR = 0
MINOR = 9

serverParameters = Parameters()


def decode_message_from_header(data_in):
    if data_in is not b'':
        frame_type, channel_number, frame_size = struct.unpack('>BHL', data_in[0:7])

    # Get the frame data
    frame_end = spec.FRAME_HEADER_SIZE + frame_size + spec.FRAME_END_SIZE

    # We don't have all of the frame yet
    if frame_end > len(data_in):
        return None

    if data_in[frame_end - 1:frame_end] != byte(spec.FRAME_END):
        raise exceptions.InvalidFrameError("Invalid FRAME_END marker")

    data_in = data_in[frame_end:]

    if data_in is not b'':
        frame_type, channel_number, frame_size = struct.unpack('>BHL', data_in[0:7])

    frame_end = spec.FRAME_HEADER_SIZE + frame_size + spec.FRAME_END_SIZE

    if frame_end > len(data_in):
        return None

    if data_in[frame_end - 1:frame_end] != byte(spec.FRAME_END):
        raise exceptions.InvalidFrameError("Invalid FRAME_END marker")

    frame_data = data_in[spec.FRAME_HEADER_SIZE:frame_end - 1]

    return frame_data


def decode_message_from_body(data_in):
    if data_in is not b'':
        frame_type, channel_number, frame_size = struct.unpack('>BHL', data_in[0:7])

    frame_end = spec.FRAME_HEADER_SIZE + frame_size + spec.FRAME_END_SIZE

    if frame_end > len(data_in):
        return None

    if data_in[frame_end - 1:frame_end] != byte(spec.FRAME_END):
        raise exceptions.InvalidFrameError("Invalid FRAME_END marker")

    frame_data = data_in[spec.FRAME_HEADER_SIZE:frame_end - 1]

    return frame_data


def check_for_existing(array, name):
    for i in array:
        if name == i.name:
            return 0
    return 1


def find_item(item, array):
    for i in array:
        if i.name == item:
            return i
    return None


