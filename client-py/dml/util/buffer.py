import math


class ByteBuffer:
    def __init__(self, value):
        if type(value) is bytearray:
            self.memory = value
        else:
            self.memory = bytearray(value)
        self.position = 0

    @staticmethod
    def allocate(capacity):
        memory = bytearray(capacity)
        return ByteBuffer(memory)

    @staticmethod
    def encode(value, length=0):
        """
        Converts the given value into its byte representation.
        """
        value_type = type(value)
        if value_type == str:
            return value.encode('utf8')
        if value_type == int:
            if length == 0:
                length = 1 if value == 0 else int(math.log(value, 256)) + 1
            return int.to_bytes(value, length, byteorder='big', signed=True)
        if value_type == list:
            return bytes(value)
        if value_type == bytes or value_type == bytearray:
            return value
        raise TypeError('Trying to convert an unknown type into bytes')

    @staticmethod
    def byte_length_of(value):
        """
        Returns the length of the given value in bytes.
        """
        return len(ByteBuffer.encode(value))

    def get_bytes(self, length=1):
        value = self.memory[self.position:self.position + length]
        self.position += length
        return value

    def get_string(self, length=1):
        return self.get_bytes(length).decode('utf8')

    def get_int(self, length=4):
        return int.from_bytes(self.get_bytes(length=length), byteorder='big', signed=True)

    def put(self, value, length=0):
        encoded_value = ByteBuffer.encode(value, length)
        length = len(encoded_value)
        self.memory[self.position:self.position + length] = encoded_value
        self.position += length
        return length

    def put_length_prefixed(self, value, prefix_length=4):
        encoded_value = ByteBuffer.encode(value, 0)
        length = len(encoded_value)
        self.put(length, prefix_length)
        self.memory[self.position:self.position + length] = encoded_value
        self.position += length
        return length

    def set(self, position, value, length=0):
        encoded_value = ByteBuffer.encode(value, length)
        length = len(encoded_value)
        self.memory[position:position + length] = encoded_value
        return length
