import os

import pytest

from dml.client import DmlClient


@pytest.fixture()
def dml():
    host = os.environ.get('DML_HOSTNAME', 'localhost')
    port = int(os.environ.get('DML_PORT', '9000'))
    client = DmlClient(host, port)
    client.connect()
    yield client
    client.disconnect()
