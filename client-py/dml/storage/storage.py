class Storage:
    """
    Storage location object
    """

    def __init__(self, storage_id, region, host, port):
        self.id = storage_id
        self.region = region
        self.host = host
        self.port = port

    def __eq__(self, other):
        if not isinstance(other, Storage):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return (self.id == other.id
                and self.region == other.region
                and self.host == other.host
                and self.port == other.port)

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        return 'Storage {' + 'id = ' + str(self.id) + \
               ', region = ' + self.region + \
               ', hostname = ' + self.host + \
               ', port = ' + str(self.port) + '}'
