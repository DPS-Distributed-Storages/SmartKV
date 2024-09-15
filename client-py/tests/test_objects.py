import pytest

from dml.exceptions import StorageCommandException, MetadataCommandException
from dml.storage.objects import SharedJson, SharedBuffer, SharedCounter


def test_obj_lock_unlock(dml):
    buf = SharedBuffer(dml, 'test_obj_lock_unlock')
    assert buf.lock_token is None
    buf.lock()
    assert buf.lock_token is not None
    value = bytes(8)
    buf.set(value)
    assert buf.get() == value
    buf.unlock()
    assert buf.lock_token is None


def test_obj_delete(dml):
    buf = SharedBuffer(dml, 'test_obj_delete')
    buf.delete()
    with pytest.raises(MetadataCommandException):
        assert buf.get()


def test_buffer_create(dml):
    buf = SharedBuffer(dml, 'test_buffer_create1')
    assert buf.get() is None
    initial_value = bytes(32)
    buf = SharedBuffer(dml, 'test_buffer_create2', value=initial_value)
    assert buf.get() == initial_value


def test_buffer_set_get(dml):
    buf = SharedBuffer(dml, 'test_buffer_set_get')
    value = bytes(8)
    buf.set(value)
    assert buf.get() == value
    buf.set(None)
    assert buf.get() is None


def test_counter_create(dml):
    counter = SharedCounter(dml, 'test_counter_create1')
    assert counter.get() == 0
    counter = SharedCounter(dml, 'test_counter_create2', value=10)
    assert counter.get() == 10


def test_counter_set_get(dml):
    counter = SharedCounter(dml, 'test_counter_set_get')
    value = 8
    counter.set(value=value)
    assert counter.get() == value


def test_counter_increment(dml):
    counter = SharedCounter(dml, 'test_counter_increment')
    counter.set(value=0)
    assert counter.increment() == 1
    assert counter.get() == 1
    assert counter.increment() == 2
    assert counter.increment(delta=5) == 7


def test_counter_decrement(dml):
    counter = SharedCounter(dml, 'test_counter_decrement')
    counter.set(value=7)
    assert counter.decrement(delta=5) == 2
    assert counter.get() == 2
    assert counter.decrement() == 1
    assert counter.get() == 1


def test_json_create(dml):
    json = SharedJson(dml, 'test_json_create1')
    assert json.get() == {}
    initial_value = {'test': 321}
    json = SharedJson(dml, 'test_json_create2', json=initial_value)
    assert json.get() == initial_value


def test_json_set_get(dml):
    json = SharedJson(dml, 'test_json_set_get')
    value = {'test': 123}
    json.set(value)
    assert json.get() == value
    json.set(124, path='$.test')
    assert json.get(path='$.test') == 124
    json.set(test_data)
    assert json.get() == test_data


def test_json_get_path(dml):
    json = SharedJson(dml, 'test_json_get_path')
    json.set(test_data)
    assert json.get(path='$.store') == test_data['store']
    assert json.get(path='$.store.book[0].author') == 'Nigel Rees'
    assert json.get(path='$..book[1].category') == ['fiction']
    assert json.get(path='$.store.bicycle.price') == pytest.approx(19.95)
    assert len(json.get(path='$..book[?(@.isbn)]')) == 2
    with pytest.raises(StorageCommandException):
        assert json.get(path='$.non.existing.path')


def test_json_put_get(dml):
    json = SharedJson(dml, 'test_json_put_get')
    json.set(test_data)
    json.put('test', 10, path='$.store')
    assert json.get(path='$.store.test') == 10


def test_json_delete(dml):
    json = SharedJson(dml, 'test_json_delete')
    json.set(test_data)
    json.delete('$.store.book[1:]')
    assert len(json.get(path='$.store.book')) == 1
    json.delete()
    assert json.get() == {}


# taken from https://github.com/json-path/JsonPath
test_data = {
    'store': {
        'book': [
            {
                'category': 'reference',
                'author': 'Nigel Rees',
                'title': 'Sayings of the Century',
                'price': 8.95
            },
            {
                'category': 'fiction',
                'author': 'Evelyn Waugh',
                'title': 'Sword of Honour',
                'price': 12.99
            },
            {
                'category': 'fiction',
                'author': 'Herman Melville',
                'title': 'Moby Dick',
                'isbn': '0-553-21311-3',
                'price': 8.99
            },
            {
                'category': 'fiction',
                'author': 'J. R. R. Tolkien',
                'title': 'The Lord of the Rings',
                'isbn': '0-395-19395-8',
                'price': 22.99
            }
        ],
        'bicycle': {
            'color': 'red',
            'price': 19.95
        }
    },
    'expensive': 10
}
