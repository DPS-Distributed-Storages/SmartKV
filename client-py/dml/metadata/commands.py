import json
from enum import Enum

from dml.exceptions import MetadataCommandException
from dml.metadata.metadata import KeyConfiguration
from dml.storage.storage import Storage
from dml.util.buffer import ByteBuffer


class FieldLength(Enum):
    MSG_LENGTH_PREFIX = 4
    REQUEST_ID = 4
    CMD_TYPE = 1
    KEY_LENGTH_PREFIX = 4
    RESULT_TYPE = 1
    NUM_NODE_IDS = 4
    NUM_FULL_REPLICATION = 4
    NODE_ID = 4
    ERROR_TYPE = 4
    LOCK_TOKEN = 4
    ID = 4
    REGION_LENGTH_PREFIX = 4
    HOST_LENGTH = 4
    PORT = 4
    METADATA_VERSION_LENGTH = 4
    NUM_REPLICAS = 4
    NUM_KEYS = 4
    SIZE_BYTES = 8


class RequestType(Enum):
    NONE = 0
    CREATE = 1
    GET = 2
    DELETE = 3
    RECONFIGURE = 4
    SYNCHRONIZED_RECONFIGURE = 5
    GETALL = 6
    GET_MEMBERSHIP_VIEW = 7
    GET_ZONE_INFO = 8
    GET_FREE_STORAGE_NODES = 9


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
    msg_buffer.put_length_prefixed(key, FieldLength.KEY_LENGTH_PREFIX.value)
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
    # Set the message length in the header
    msg_buffer.set(0, msg_buffer.position - FieldLength.MSG_LENGTH_PREFIX.value, FieldLength.MSG_LENGTH_PREFIX.value)


def _decode_reply_header(buffer):
    _ = buffer.get_int(FieldLength.REQUEST_ID.value)
    result_type = buffer.get_int(FieldLength.RESULT_TYPE.value)
    if result_type == ResultType.ERROR.value:
        error_type = buffer.get_int(FieldLength.ERROR_TYPE.value)
        raise MetadataCommandException(error_type)


class MetadataCreate:
    def __init__(self, key, replicas, full_replication):
        self.key = key
        self.replicas = replicas
        self.full_replication = full_replication

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(ByteBuffer.byte_length_of(self.key) + 20, request_id,
                                      RequestType.CREATE.value, self.key)
        msg_buffer.put(int(self.full_replication), FieldLength.NUM_FULL_REPLICATION.value)
        if not self.replicas:
            msg_buffer.put(-1, FieldLength.NUM_NODE_IDS.value)
        else:
            msg_buffer.put(len(self.replicas), FieldLength.NUM_NODE_IDS.value)
            for r in self.replicas:
                msg_buffer.put(r, FieldLength.NODE_ID.value)

        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        _decode_reply_header(buffer)


class MetadataDelete:
    def __init__(self, key):
        self.key = key

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(ByteBuffer.byte_length_of(self.key) + 20, request_id,
                                      RequestType.DELETE.value, self.key)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        _decode_reply_header(buffer)


class MetadataGet:
    def __init__(self, key):
        self.key = key

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(ByteBuffer.byte_length_of(self.key) + 20,
                                      request_id, RequestType.GET.value, self.key)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        _decode_reply_header(buffer)
        metadata_version = buffer.get_int(FieldLength.METADATA_VERSION_LENGTH.value)
        num_replicas = buffer.get_int(FieldLength.NUM_REPLICAS.value)
        replicas = []
        for _ in range(num_replicas):
            storage_id = buffer.get_int(FieldLength.ID.value)
            region_length = buffer.get_int(FieldLength.REGION_LENGTH_PREFIX.value)
            region = buffer.get_string(region_length)
            host_length = buffer.get_int(FieldLength.HOST_LENGTH.value)
            host = buffer.get_string(host_length)
            port = buffer.get_int(FieldLength.PORT.value)
            storage = Storage(storage_id, region, host, port)
            replicas.append(storage)
        return KeyConfiguration(metadata_version, replicas)


class MetadataGetAll:

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer_empty(20, request_id, RequestType.GETALL.value)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        _decode_reply_header(buffer)
        num_keys = buffer.get_int(FieldLength.NUM_KEYS.value)
        result = {}
        for _ in range(num_keys):
            key_length = buffer.get_int(FieldLength.KEY_LENGTH_PREFIX.value)
            key = buffer.get_string(key_length)
            metadata_version = buffer.get_int(FieldLength.METADATA_VERSION_LENGTH.value)
            num_replicas = buffer.get_int(FieldLength.NUM_REPLICAS.value)
            replicas = []
            for _ in range(num_replicas):
                storage_id = buffer.get_int(FieldLength.ID.value)
                region_length = buffer.get_int(FieldLength.REGION_LENGTH_PREFIX.value)
                region = buffer.get_string(region_length)
                host_length = buffer.get_int(FieldLength.HOST_LENGTH.value)
                host = buffer.get_string(host_length)
                port = buffer.get_int(FieldLength.PORT.value)
                storage = Storage(storage_id, region, host, port)
                replicas.append(storage)
            result[key] = KeyConfiguration(metadata_version, replicas)
        return result


class MetadataReconfigure:
    def __init__(self, key, replicas):
        self.key = key
        self.replicas = replicas

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(ByteBuffer.byte_length_of(self.key) + 20,
                                      request_id, RequestType.RECONFIGURE.value, self.key)

        if not self.replicas:
            msg_buffer.put(-1, FieldLength.NUM_NODE_IDS.value)
        else:
            msg_buffer.put(len(self.replicas), FieldLength.NUM_NODE_IDS.value)
            for r in self.replicas:
                msg_buffer.put(r, FieldLength.NODE_ID.value)

        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        _decode_reply_header(buffer)


class MetadataSynchronizedReconfigure:
    def __init__(self, key, seen_replicas, new_replicas):
        self.key = key
        self.seen_replicas = seen_replicas
        self.new_replicas = new_replicas

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(ByteBuffer.byte_length_of(self.key) + 20,
                                      request_id, RequestType.SYNCHRONIZED_RECONFIGURE.value, self.key)

        if not self.seen_replicas:
            msg_buffer.put(-1, FieldLength.NUM_NODE_IDS.value)
        else:
            msg_buffer.put(len(self.seen_replicas), FieldLength.NUM_NODE_IDS.value)
            for r in self.seen_replicas:
                msg_buffer.put(r, FieldLength.NODE_ID.value)

        if not self.new_replicas:
            msg_buffer.put(-1, FieldLength.NUM_NODE_IDS.value)
        else:
            msg_buffer.put(len(self.new_replicas), FieldLength.NUM_NODE_IDS.value)
            for r in self.new_replicas:
                msg_buffer.put(r, FieldLength.NODE_ID.value)

        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        _decode_reply_header(buffer)


class MetadataGetMembershipView:
    @staticmethod
    def encode(request_id):
        msg_buffer = _init_msg_buffer_empty(20, request_id, RequestType.GET_MEMBERSHIP_VIEW.value)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        _decode_reply_header(buffer)
        membership_view_json_length = buffer.get_int(FieldLength.HOST_LENGTH.value)
        membership_view_json = buffer.get_string(membership_view_json_length)
        return json.loads(membership_view_json)


class MetadataGetZoneInfo:
    def __init__(self, zone):
        self.zone = zone

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(ByteBuffer.byte_length_of(self.zone) + 20,
                                      request_id, RequestType.GET_ZONE_INFO.value, self.zone)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        _decode_reply_header(buffer)
        zone_info_json_length = buffer.get_int(FieldLength.HOST_LENGTH.value)
        zone_info_json = buffer.get_string(zone_info_json_length)
        return json.loads(zone_info_json)


class MetadataGetFreeStorageNodes:
    def __init__(self, zones, object_size_in_bytes):
        self.zones = zones
        self.object_size_in_bytes = object_size_in_bytes

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer_empty(20, request_id, RequestType.GET_FREE_STORAGE_NODES.value)
        msg_buffer.put(self.object_size_in_bytes, FieldLength.SIZE_BYTES.value)
        if not self.zones:
            msg_buffer.put(-1, FieldLength.NUM_NODE_IDS.value)
        else:
            msg_buffer.put(len(self.zones), FieldLength.NUM_NODE_IDS.value)
            for z in self.zones:
                msg_buffer.put_length_prefixed(z, FieldLength.HOST_LENGTH.value)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        _decode_reply_header(buffer)
        storage_ids_json_length = buffer.get_int(FieldLength.HOST_LENGTH.value)
        storage_ids_info_json = buffer.get_string(storage_ids_json_length)
        return json.loads(storage_ids_info_json)