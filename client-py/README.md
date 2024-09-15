# Python client

## Installation

In this directory, run the following command to install the package:

```bash
pip install .
```

## Usage

### Synchronous client

```python
from dml.client import DmlClient
from dml.storage.objects import SharedCounter, SharedJson


dml = DmlClient('localhost', 9000)
dml.connect()

# storing and retrieving bytes
dml.create('key')
value = 'hello'.encode()
dml.set('key', value)
print(dml.get('key').decode())

# shared counter
counter = SharedCounter(dml, 'counter')
print(counter.increment(1))
print(counter.increment(1))

# shared JSON
data = {
    'store': {
        'book': [
            {
                'category': 'reference',
                'author': 'Nigel Rees',
                'title': 'Sayings of the Century',
                'price': 8.95
            }
        ],
        'bicycle': {
            'color': 'red',
            'price': 19.95
        }
    },
    'expensive': 10
}
json = SharedJson(dml, 'json')
json.set(data)
print(json.get(path='$.store.book[0].author'))

dml.disconnect()
```


### Asynchronous client

```python
import asyncio

from dml.asyncio.client import DmlClient
from dml.asyncio.objects import SharedJson


async def main():
    dml = DmlClient('localhost', 9000)
    await dml.connect()

    # storing and retrieving bytes
    await dml.create('key')
    value = 'hello'.encode()
    await dml.set('key', value)
    print((await dml.get('key')).decode())

    # shared JSON
    data = {
        'store': {
            'book': [
                {
                    'category': 'reference',
                    'author': 'Nigel Rees',
                    'title': 'Sayings of the Century',
                    'price': 8.95
                }
            ],
            'bicycle': {
                'color': 'red',
                'price': 19.95
            }
        },
        'expensive': 10
    }
    json = await SharedJson.create(dml, 'json')
    await json.set(data)
    print(await json.get(path='$.store.book[0].author'))

    await dml.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
```


## Development

Install the package in `editable` mode:

```bash
pip install -e .
```
