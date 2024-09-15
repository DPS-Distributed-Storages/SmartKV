import pytest

from dml.exceptions import MetadataCommandException


def test_get_membership_view(dml):
    membership_view = dml.get_membership_view()
    assert 'epoch' in membership_view


def test_create(dml):
    key = 'test_create'
    dml.create(key)
    assert dml.get(key)['buffer'] is None


def test_invoke_method_key_set(dml):
    keys = ['test_invoke_method_key_set_1', 'test_invoke_method_key_set_2', 'test_invoke_method_key_set_3']

    for key in keys:
        dml.create(key, 'SharedCounter', args=[0])
        assert dml.get(key) is not None

    increment = int(3)
    dml.invoke_method(keys, 'set', [0])
    results = dml.invoke_method(keys, 'increment', [increment])

    for idx, key in enumerate(keys):
        assert results[idx] == increment
        assert dml.get(key)["value"] == increment


def test_register_and_use_lua(dml):
    class_name = 'Rectangle'
    with open('./resources/Rectangle.lua', 'r') as rectangle_lua:
        byte_code = bytes(rectangle_lua.read(), 'utf-8')
    dml.register_shared_class(class_name, byte_code, 'lua')
    assert dml.get(class_name)['className'] == class_name

    key = 'rect1'
    length, breadth = 10, 20
    dml.create(key, class_name, 'lua', [length, breadth], None, True, True)
    area = float(dml.invoke_method(key, 'getArea', None))
    assert abs(area - length * breadth) < 1e-06


def test_register_and_use_java(dml):
    class_name = 'SharedStatistics'
    with open('./resources/SharedStatistics.class', 'rb') as shared_statistics_java:
        byte_code = shared_statistics_java.read()
    dml.register_shared_class(class_name, byte_code, 'java')
    assert dml.get(class_name)['className'] == class_name

    key = 'statistic1'
    value1, value2 = 3, 9
    dml.create(key, class_name, 'java', None, None, True, True)
    is_added = bool(dml.invoke_method(key, 'add', args=[value1]))
    assert is_added
    is_added = bool(dml.invoke_method(key, 'add', args=[value2]))
    assert is_added
    average = float(dml.invoke_method(key, 'average'))
    assert abs(average - (value1 + value2) / 2.0) < 1e-6


def test_set_get(dml):
    key = 'test_set_get'
    value = bytes(32)
    dml.create(key)
    dml.set(key, [value])
    assert dml.get(key)['buffer'] == value
    dml.set(key, None)
    assert dml.get(key)['buffer'] is None


def test_lock_unlock(dml):
    key = 'test_lock_unlock'
    value = bytes(8)
    dml.create(key)
    dml.set(key, value)
    lock_token = dml.lock(key)
    dml.unlock(key, lock_token)
    assert dml.get(key)['buffer'] == value


def test_delete(dml):
    key = 'test_delete'
    dml.create(key)
    dml.delete(key)
    with pytest.raises(MetadataCommandException):
        assert dml.get(key)


def test_get_zone_info(dml):
    zone_info = dml.get_zone_info('local_zone1')
    assert 'weightedAverageUnitPrices' in zone_info
    assert 'totalFreeMemory' in zone_info
    print(zone_info)


def test_get_free_storage_nodes(dml):
    zones = ['local_zone1']
    storage_ids = dml.get_free_storage_nodes(zones, 1024)
    assert storage_ids is not None
    assert len(storage_ids) > 0
    print(storage_ids)