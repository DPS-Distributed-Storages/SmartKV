from enum import Enum

from dml.util.buffer import ByteBuffer


class OptimizerCommandErrorCode(Enum):
    UNKNOWN_ERROR = 0
    UNKNOWN_COMMAND = 1


class OptimizerCommandException(Exception):
    def __init__(self, num, msg=None):
        self.num = num
        self.msg = msg
        super().__init__(self.error())

    def error(self):
        if self.num == OptimizerCommandErrorCode.UNKNOWN_ERROR.value:
            return 'Unknown error'
        if self.num == OptimizerCommandErrorCode.UNKNOWN_COMMAND.value:
            return 'Unknown command'


class FieldLength(Enum):
    MSG_LENGTH_PREFIX = 4
    REQUEST_ID = 4
    CMD_TYPE = 1
    KEY_LENGTH_PREFIX = 4
    ZONE_LENGTH_PREFIX = 4
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
    NUM_TCP_REQUEST_TYPES = 4
    TCP_REQUEST_TYPE = 4
    NUM_REQUESTS = 4
    NUM_REQUESTS_LONG = 8
    NUM_BYTES_LONG = 8
    TIMESTAMP_LONG = 8
    NUM_MESSAGES_RECEIVED_OR_SENT = 4
    NUM_KEYS = 4
    NUM_RECORDS = 4
    STRING = 4
    RESULT_LENGTH_PREFIX = 4


class RequestType(Enum):
    GET_STATISTICS = 0
    GET_MAIN_OBJECT_LOCATIONS = 1
    GET_UNIT_PRICES = 2
    GET_BILLS = 3
    CLEAR_STATISTICS = 4
    GET_FREE_MEMORY = 5


class ResultType(Enum):
    SUCCESS = 0
    ERROR = 1


def _init_msg_buffer(length, request_id, request_type):
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
        raise OptimizerCommandException(error_type)


class GetStatistic:

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(20, request_id, RequestType.GET_STATISTICS.value)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        _decode_reply_header(buffer)
        storage_zone_length = buffer.get_int(FieldLength.ZONE_LENGTH_PREFIX.value)
        storage_zone = buffer.get_string(storage_zone_length)
        num_records = buffer.get_int(FieldLength.NUM_RECORDS.value)
        result = {}
        result[storage_zone] = {}
        for _ in range(num_records):
            time_stamp = buffer.get_int(FieldLength.TIMESTAMP_LONG.value)
            num_entries = buffer.get_int(FieldLength.NUM_KEYS.value)
            result[storage_zone][time_stamp] = {}
            for _ in range(num_entries):
                key_length = buffer.get_int(FieldLength.KEY_LENGTH_PREFIX.value)
                key = buffer.get_string(key_length)
                zone_length = buffer.get_int(FieldLength.ZONE_LENGTH_PREFIX.value)
                zone = buffer.get_string(zone_length)
                if key not in result[storage_zone][time_stamp]:
                    result[storage_zone][time_stamp][key] = {}
                num_requests = buffer.get_int(FieldLength.NUM_REQUESTS.value)
                requests = {}
                for _ in range(num_requests):
                    tcp_request_type_length = buffer.get_int(FieldLength.TCP_REQUEST_TYPE.value)
                    tcp_request_type = buffer.get_string(tcp_request_type_length)
                    num_requests = buffer.get_int(FieldLength.NUM_REQUESTS_LONG.value)
                    requests[tcp_request_type] = num_requests
                result[storage_zone][time_stamp][key][zone] = {'num_requests': requests, 'cummulative_read_bytes': {},
                                                 'cummulative_sent_bytes': {}}

                num_messages_read = buffer.get_int(FieldLength.NUM_MESSAGES_RECEIVED_OR_SENT.value)
                messages = {}
                for _ in range(num_messages_read):
                    tcp_request_type_length = buffer.get_int(FieldLength.TCP_REQUEST_TYPE.value)
                    tcp_request_type = buffer.get_string(tcp_request_type_length)
                    num_bytes = buffer.get_int(FieldLength.NUM_BYTES_LONG.value)
                    messages[tcp_request_type] = num_bytes
                result[storage_zone][time_stamp][key][zone]['cummulative_read_bytes'] = messages

                num_messages_sent = buffer.get_int(FieldLength.NUM_MESSAGES_RECEIVED_OR_SENT.value)
                messages = {}
                for _ in range(num_messages_sent):
                    tcp_request_type_length = buffer.get_int(FieldLength.TCP_REQUEST_TYPE.value)
                    tcp_request_type = buffer.get_string(tcp_request_type_length)
                    num_bytes = buffer.get_int(FieldLength.NUM_BYTES_LONG.value)
                    messages[tcp_request_type] = num_bytes
                result[storage_zone][time_stamp][key][zone]['cummulative_sent_bytes'] = messages
        return result


class GetMainObjectLocations:

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(20, request_id, RequestType.GET_MAIN_OBJECT_LOCATIONS.value)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    @staticmethod
    def decode_reply(buffer):
        _decode_reply_header(buffer)
        num_entries = buffer.get_int(FieldLength.NUM_KEYS.value)
        result = {}
        for _ in range(num_entries):
            key_length = buffer.get_int(FieldLength.KEY_LENGTH_PREFIX.value)
            key = buffer.get_string(key_length)
            num_locations = buffer.get_int(FieldLength.NUM_NODE_IDS.value)
            locations = []
            for _ in range(num_locations):
                storage_id = buffer.get_int(FieldLength.ID.value)
                locations.append(storage_id)
            result[key] = locations
        return result


class GetUnitPrices:

    def __init__(self, args_codec):
        self.args_codec = args_codec

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(20, request_id, RequestType.GET_UNIT_PRICES.value)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    def decode_reply(self, buffer):
        _decode_reply_header(buffer)
        result_length = buffer.get_int(FieldLength.RESULT_LENGTH_PREFIX.value)
        if result_length <= 0:
            return None
        encoded_result = buffer.get_bytes(result_length)
        return self.args_codec.decode(encoded_result)[0]


class GetBills:

    def __init__(self, args_codec):
        self.args_codec = args_codec

    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(20, request_id, RequestType.GET_BILLS.value)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    def decode_reply(self, buffer):
        _decode_reply_header(buffer)
        result_length = buffer.get_int(FieldLength.RESULT_LENGTH_PREFIX.value)
        if result_length <= 0:
            return None
        encoded_result = buffer.get_bytes(result_length)
        return self.args_codec.decode(encoded_result)[0]


class GetFreeMemory:
    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(20, request_id, RequestType.GET_FREE_MEMORY.value)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    def decode_reply(self, buffer):
        _decode_reply_header(buffer)
        free_memory = buffer.get_int(FieldLength.NUM_BYTES_LONG.value)
        return free_memory


class ClearStatistics:
    def encode(self, request_id):
        msg_buffer = _init_msg_buffer(20, request_id, RequestType.CLEAR_STATISTICS.value)
        _update_msg_header(msg_buffer)
        return [msg_buffer.memory[:msg_buffer.position]]

    def decode_reply(self, buffer):
        _decode_reply_header(buffer)
        return None
