from pika import spec
import struct
from pika import exceptions
from pika.compat import byte
from pika.connection import Parameters
import random
import string

HOSTNAME = 'localhost'
MAX_BYTES = 131072
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


def random_queue_name_gen():
    basic_str = 'amq.gen-'
    random_str = string.ascii_letters
    pick_some = ''
    for i in range(11):
        pick_some += random_str[i]

    whole_string = basic_str + pick_some

    return whole_string


def find_topic_routing_keys(array):
    list_of_keys = []
    list_of_topic_keys = []
    for i in array:
        list_of_keys.append(i[2])

    for routing_key in list_of_keys:
        for i in routing_key:
            if i == '.':
                list_of_topic_keys.append(routing_key)
                break
    return list_of_topic_keys


def check_for_binding(list_of_bindings, key):
    routing_key = key.split('.')
    list_of_checked_keys = []
    for i in list_of_bindings:
        split_key = i.split('.')
        if check_for_sign(split_key, '*'):
            if compare_keys_for_star(routing_key, split_key):
                list_of_checked_keys.append(i)
        if check_for_sign(split_key, '#'):
            if compare_key_for_hash(routing_key, split_key):
                list_of_checked_keys.append(i)
    return list_of_checked_keys


def check_for_sign(list_of_strings, sign):
    no_sign = False
    for i in list_of_strings:
        if i == sign:
            no_sign = True
    return no_sign


def compare_keys_for_star(string1, string2):
    if len(string1) is not len(string2):
        return False
    for i in range(len(string1)):
        if (string1[i] == string2[i]) or (string2[i] == '*'):
            print('ok')
        else:
            return False
    return True


def compare_key_for_hash(string1, string2):
    if string1[0] == string2[0]:
        return True
    else:
        return False


def return_header(data_in):
    try:
        (frame_type, channel_number, frame_size) = struct.unpack('>BHL', data_in[0:7])
    except struct.error:
        return 0

    frame_end = spec.FRAME_HEADER_SIZE + frame_size + spec.FRAME_END_SIZE

    return data_in[frame_end:]


def return_body(data_in):
    try:
        (frame_type, channel_number, frame_size) = struct.unpack('>BHL', data_in[0:7])
    except struct.error:
        return 0

    frame_end = spec.FRAME_HEADER_SIZE + frame_size + spec.FRAME_END_SIZE

    return data_in[frame_end:]


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
