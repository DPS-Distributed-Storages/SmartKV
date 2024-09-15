import os

import pytest_asyncio

from dml.asyncio.client import DmlClient


@pytest_asyncio.fixture()
async def dml():
    host = os.environ.get('DML_HOSTNAME', 'localhost')
    port = int(os.environ.get('DML_PORT', '9000'))
    client = DmlClient(host, port)
    await client.connect()
    yield client
    await client.disconnect()
