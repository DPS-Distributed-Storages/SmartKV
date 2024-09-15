from enum import Enum, IntFlag

from dml.exceptions import StorageCommandException
from dml.util.NodeLocation import NodeLocation
from dml.util.buffer import ByteBuffer


class FieldLength(Enum):
    MSG_LENGTH_PREFIX = 4
    REQUEST_ID = 4
    CMD_TYPE = 1
    STRING_LENGTH_PREFIX = 4
    RESULT_TYPE = 1
    METADATA_VERSION_LENGTH = 4
    ARGS_LENGTH_PREFIX = 4
    RESULT_LENGTH_PREFIX = 4
    ERROR_TYPE = 4
    LOCK_TOKEN = 4
    FLAGS = 1


class RequestType(Enum):
    LOCK = 0
    UNLOCK = 1
    INIT_OBJECT = 2
    INVOKE_METHOD = 3
    SET = 4
    GET = 5
    PUSH_CLIENT_LOCATION = 6


class CommandFlag(IntFlag):
    NONE = 0

    # Flag values need to be powers of 2

    READ_ONLY = 1 << 0
    """Indicates that the command does not have side effects and therefore does not need to be replicated."""

    ALLOW_INVALID_READS = 1 << 1
    """
    If this flag is set, a read request is allowed to return data from an object in invalid state. 
    Only has an effect in combination with the READ_ONLY flag.
    """

    ASYNC_REPLICATION = 1 << 2
    """If this flag is set, the command returns the result before replication is completed."""


class ResultType(Enum):
    SUCCESS = 0
    ERROR = 1


def _init_msg_buffer(length, request_id, request_type, key):
    msg_buffer = ByteBuffer.allocate(length)
    # Msg length (4 bytes)
    msg_length = 0
    msg_buffer.put(msg_length, FieldLength.MSG_LENGTH_PREFIX.value)
    # Request ID (4 bytes)
    msg_buffer.put(request_id, FieldLength.REQUEST_ID.value)
    # Request type (1 bytes)
    msg_buffer.put(request_type, FieldLength.CMD_TYPE.value)
    # Key length (4 bytes) + Key (x bytes)
    msg_buffer.put_length_prefixed(key, FieldLength.STRING_LENGTH_PREFIX.value)
    return msg_buffer


def _init_msg_buffer_empty(length, request_id, request_type):
    msg_buffer = ByteBuffer.allocate(length)
    # Msg length (4 bytes)
    msg_length = 0
    msg_buffer.put(msg_length, FieldLength.MSG_LENGTH_PREFIX.value)
    # Request ID (4 bytes)
    msg_buffer.put(request_id, FieldLength.REQUEST_ID.value)
    # Request type (1 bytes)
    msg_buffer.put(request_type, FieldLength.CMD_TYPE.value)
    return msg_buffer


def _update_msg_header(msg_buffer):
    # Set the message length
    msg_buffer.set(0, msg_buffer.position - FieldLength.MSG_LENGTH_PREFIX.value, FieldLength.MSG_LENGTH_PREFIX.value)


def _decode_reply_header(buffer):
    _ = buffer.get_int(FieldLength.REQUEST_ID.value)
    result_type = buffer.get_int(FieldLength.RESULT_TYPE.value)
    if result_type == ResultType.ERROR.value:
        error_code = buffer.get_int(FieldLength.ERROR_TYPE.value)
        error_msg_length = buffer.get_int(FieldLength.STRING_LENGTH_PREFIX.value)
        error_msg = buffer.get_string(error_msg_length)
        raise StorageCommandException(error_code, error_msg)
    return buffer.get_int(FieldLength.METADATA_VERSION_LENGTH.value)


class StorageInitObject:
    def __init__(self, args_codec, key, language_id, object_type, args, lock_token):
        self.args_codec = args_codec
        self.key = key
        self.language_id = language_id
        self.object_type = object_type
        self.args = args
        self.lock_token = lock_token

    def encode(self, request_id):
        msg_header_buffer = _init_msg_buffer(ByteBuffer.byte_length_of(self.key) +
                                             ByteBuffer.byte_length_of(self.language_id) +
                                             ByteBuffer.byte_length_of(self.object_type) + 20,
                                             request_id, RequestType.INIT_OBJECT.value, self.key)

        msg_header_buffer.put_length_prefixed(self.language_id, FieldLength.STRING_LENGTH_PREFIX.value)
        msg_header_buffer.put_length_prefixed(self.object_type, FieldLength.STRING_LENGTH_PREFIX.value)

        encoded_args = None
        if self.args is None:
            msg_header_buffer.put(-1, FieldLength.ARGS_LENGTH_PREFIX.value)
        else:
            encoded_args = self.args_codec.encode(self.args)
            msg_header_buffer.put(len(encoded_args), FieldLength.ARGS_LENGTH_PREFIX.value)

        header_length = msg_header_buffer.position
        msg_length = header_length
        if self.args is not None:
            msg_length += len(encoded_args)
        if self.lock_token is not None:
            msg_length += FieldLength.LOCK_TOKEN.value

        msg_header_buffer.set(0, msg_length - FieldLength.MSG_LENGTH_PREFIX.value, FieldLength.MSG_LENGTH_PREFIX.value)

        msg_chunks = [msg_header_buffer.memory[:header_length]]
        if self.args is not None:
            msg_chunks.append(encoded_args)
        if self.lock_token is not None:
            msg_chunks.append(ByteBuffer.encode(self.lock_token, FieldLength.LOCK_TOKEN.value))
        return msg_chunks

    @staticmethod
    def decode_reply(buffer):
        metadata_version = _decode_reply_header(buffer)
        return metadata_version, None


class StorageInvokeMethod:
    def __init__(self, args_codec, key, method_name, args, lock_token, flags):
        self.args_codec = args_codec
        self.key = key
        self.method_name = method_name
        self.args = args
        self.lock_token = lock_token
        self.flags = flags

    def encode(self, request_id):
        msg_header_buffer = _init_msg_buffer(ByteBuffer.byte_length_of(self.key) +
                                             ByteBuffer.byte_length_of(self.method_name) + 20,
                                             request_id, RequestType.INVOKE_METHOD.value, self.key)

        msg_header_buffer.put_length_prefixed(self.method_name, FieldLength.STRING_LENGTH_PREFIX.value)

        encoded_args = None
        if self.args is None:
            msg_header_buffer.put(-1, FieldLength.ARGS_LENGTH_PREFIX.value)
        else:
            encoded_args = self.args_codec.encode(self.args)
            msg_header_buffer.put(len(encoded_args), FieldLength.ARGS_LENGTH_PREFIX.value)

        header_length = msg_header_buffer.position
        msg_length = header_length
        if self.args is not None:
            msg_length += len(encoded_args)
        msg_length += FieldLength.FLAGS.value
        if self.lock_token is not None:
            msg_length += FieldLength.LOCK_TOKEN.value

        msg_header_buffer.set(0, msg_length - FieldLength.MSG_LENGTH_PREFIX.value, FieldLength.MSG_LENGTH_PREFIX.value)

        msg_chunks = [msg_header_buffer.memory[:header_length]]
        if self.args is not None:
            msg_chunks.append(encoded_args)
        flags_bit_vector = int(self.flags)
        msg_chunks.append(ByteBuffer.encode(flags_bit_vector, FieldLength.FLAGS.value))
        if self.lock_token is not None:
            msg_chunks.append(ByteBuffer.encode(self.lock_token, FieldLength.LOCK_TOKEN.value))
        return msg_chunks

    def decode_reply(self, buffer):
        metadata_version = _decode_reply_header(buffer)
        result_length = buffer.get_int(FieldLength.RESULT_LENGTH_PREFIX.value)
        if result_length <= 0:
            return None
        encoded_result = buffer.get_bytes(result_length)
        return metadata_version, self.args_codec.decode(encoded_result)[0]


class StorageLock:
    def __init__(self, key):
        self.key = key

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(ByteBuffer.byte_length_of(self.key) + 20,
                                      request_id, RequestType.LOCK.value, self.key)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        metadata_version = _decode_reply_header(buffer)
        lock_token = buffer.get_int(FieldLength.LOCK_TOKEN.value)
        return metadata_version, lock_token


class StorageUnlock:
    def __init__(self, key, lock_token):
        self.key = key
        self.lock_token = lock_token

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(ByteBuffer.byte_length_of(self.key) + 20,
                                      request_id, RequestType.UNLOCK.value, self.key)
        msg_buffer.put(self.lock_token, FieldLength.LOCK_TOKEN.value)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        metadata_version = _decode_reply_header(buffer)
        return metadata_version, None


class PushClientLocation:
    def __init__(self, client_location: NodeLocation):
        self.client_location = client_location

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer_empty(ByteBuffer.byte_length_of(self.client_location.region) +
                                            ByteBuffer.byte_length_of(self.client_location.access_point) +
                                            ByteBuffer.byte_length_of(self.client_location.provider) + 20,
                                            request_id, RequestType.PUSH_CLIENT_LOCATION.value)
        msg_buffer.put_length_prefixed(self.client_location.region, FieldLength.STRING_LENGTH_PREFIX.value)
        msg_buffer.put_length_prefixed(self.client_location.access_point, FieldLength.STRING_LENGTH_PREFIX.value)
        msg_buffer.put_length_prefixed(self.client_location.provider, FieldLength.STRING_LENGTH_PREFIX.value)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        return None


class StorageGet:
    def __init__(self, args_codec, key, lock_token, flags):
        self.args_codec = args_codec
        self.key = key
        self.lock_token = lock_token
        self.flags = flags

    def encode(self, request_id):
        msg_header_buffer = _init_msg_buffer(ByteBuffer.byte_length_of(self.key) + 20,
                                             request_id, RequestType.GET.value, self.key)

        header_length = msg_header_buffer.position
        msg_length = header_length

        msg_length += FieldLength.FLAGS.value
        if self.lock_token is not None:
            msg_length += FieldLength.LOCK_TOKEN.value

        msg_header_buffer.set(0, msg_length - FieldLength.MSG_LENGTH_PREFIX.value, FieldLength.MSG_LENGTH_PREFIX.value)

        msg_chunks = [msg_header_buffer.memory[:header_length]]

        flags_bit_vector = int(self.flags)
        msg_chunks.append(ByteBuffer.encode(flags_bit_vector, FieldLength.FLAGS.value))
        if self.lock_token is not None:
            msg_chunks.append(ByteBuffer.encode(self.lock_token, FieldLength.LOCK_TOKEN.value))
        return msg_chunks

    def decode_reply(self, buffer):
        metadata_version = _decode_reply_header(buffer)
        result_length = buffer.get_int(FieldLength.RESULT_LENGTH_PREFIX.value)
        if result_length <= 0:
            return None
        encoded_result = buffer.get_bytes(result_length)
        return metadata_version, self.args_codec.decode(encoded_result)[0]


class StorageSet:
    def __init__(self, args_codec, key, args, lock_token, flags):
        self.args_codec = args_codec
        self.key = key
        self.args = args
        self.lock_token = lock_token
        self.flags = flags

    def encode(self, request_id):
        msg_header_buffer = _init_msg_buffer(ByteBuffer.byte_length_of(self.key) + 20,
                                             request_id, RequestType.SET.value, self.key)

        encoded_args = None
        if self.args is None:
            msg_header_buffer.put(-1, FieldLength.ARGS_LENGTH_PREFIX.value)
        else:
            encoded_args = self.args_codec.encode(self.args)
            msg_header_buffer.put(len(encoded_args), FieldLength.ARGS_LENGTH_PREFIX.value)

        header_length = msg_header_buffer.position
        msg_length = header_length
        if self.args is not None:
            msg_length += len(encoded_args)
        msg_length += FieldLength.FLAGS.value
        if self.lock_token is not None:
            msg_length += FieldLength.LOCK_TOKEN.value

        msg_header_buffer.set(0, msg_length - FieldLength.MSG_LENGTH_PREFIX.value, FieldLength.MSG_LENGTH_PREFIX.value)

        msg_chunks = [msg_header_buffer.memory[:header_length]]
        if self.args is not None:
            msg_chunks.append(encoded_args)
        flags_bit_vector = int(self.flags)
        msg_chunks.append(ByteBuffer.encode(flags_bit_vector, FieldLength.FLAGS.value))
        if self.lock_token is not None:
            msg_chunks.append(ByteBuffer.encode(self.lock_token, FieldLength.LOCK_TOKEN.value))
        return msg_chunks

    def decode_reply(self, buffer):
        metadata_version = _decode_reply_header(buffer)
        return metadata_version, None
