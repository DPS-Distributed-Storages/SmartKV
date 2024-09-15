from enum import Enum


class MetadataCommandErrorCode(Enum):
    NO_ERROR = 0
    UNKNOWN_ERROR = 1
    UNKNOWN_COMMAND = 2
    KEY_DOES_NOT_EXIST = 3
    KEY_ALREADY_EXISTS = 4
    INVALID_OPERATION = 5
    CONCURRENT_OPERATION = 6
    INVALID_OBJECT_LOCATIONS = 7


class MetadataCommandException(Exception):
    def __init__(self, num):
        self.num = num
        super().__init__(self.error())

    def error(self):
        if self.num == MetadataCommandErrorCode.NO_ERROR.value:
            return 'No error'
        if self.num == MetadataCommandErrorCode.UNKNOWN_ERROR.value:
            return 'Unknown error'
        if self.num == MetadataCommandErrorCode.UNKNOWN_COMMAND.value:
            return 'Unknown command'
        if self.num == MetadataCommandErrorCode.KEY_DOES_NOT_EXIST.value:
            return 'Key does not exist'
        if self.num == MetadataCommandErrorCode.KEY_ALREADY_EXISTS.value:
            return 'Key already exists'
        if self.num == MetadataCommandErrorCode.INVALID_OPERATION.value:
            return 'Invalid operation'
        if self.num == MetadataCommandErrorCode.CONCURRENT_OPERATION.value:
            return 'Aborted due to a concurrent operation to the same key'
        if self.num == MetadataCommandErrorCode.INVALID_OBJECT_LOCATIONS.value:
            return 'Invalid object locations'


class StorageCommandErrorCode(Enum):
    UNKNOWN_ERROR = 0
    UNKNOWN_COMMAND = 1
    KEY_DOES_NOT_EXIST = 2
    INVALID_LOCK_TOKEN = 3
    OBJECT_NOT_INITIALIZED = 4
    NOT_RESPONSIBLE = 5
    SHARED_OBJECT_COMMAND_ERROR = 6


class StorageCommandException(Exception):
    def __init__(self, num, msg=None):
        self.num = num
        self.msg = msg
        super().__init__(self.error())

    def error(self):
        if self.num == StorageCommandErrorCode.UNKNOWN_ERROR.value:
            return 'Unknown error'
        if self.num == StorageCommandErrorCode.UNKNOWN_COMMAND.value:
            return 'Unknown command'
        if self.num == StorageCommandErrorCode.KEY_DOES_NOT_EXIST.value:
            return 'Key does not exist'
        if self.num == StorageCommandErrorCode.INVALID_LOCK_TOKEN.value:
            return 'Invalid lock token'
        if self.num == StorageCommandErrorCode.OBJECT_NOT_INITIALIZED.value:
            return 'Object not initialized'
        if self.num == StorageCommandErrorCode.NOT_RESPONSIBLE.value:
            return 'Not responsible for this key'
        if self.num == StorageCommandErrorCode.SHARED_OBJECT_COMMAND_ERROR.value:
            return self.msg
