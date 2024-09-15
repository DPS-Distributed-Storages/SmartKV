import pytest

from dml.exceptions import MetadataCommandException


@pytest.mark.asyncio
async def test_get_membership_view(dml):
    membership_view = await dml.get_membership_view()
    assert 'epoch' in membership_view


@pytest.mark.asyncio
async def test_create(dml):
    key = 'test_create'
    await dml.create(key)
    assert (await dml.get(key))['buffer'] is None


@pytest.mark.asyncio
async def test_invoke_method_key_set(dml):
    keys = ['async_test_invoke_method_key_set_1', 'async_test_invoke_method_key_set_2', 'async_test_invoke_method_key_set_3']

    for key in keys:
        await dml.create(key, 'SharedCounter', args=[0])
        assert await dml.get(key) is not None

    increment = int(3)
    await dml.invoke_method(keys, 'set', [0])
    results = await dml.invoke_method(keys, 'increment', [increment])

    for idx, key in enumerate(keys):
        assert results[idx] == increment
        assert (await dml.get(key))['value'] == increment


@pytest.mark.asyncio
async def test_register_and_use_lua(dml):
    class_name = 'Rectangle'
    with open('./resources/Rectangle.lua', 'r') as rectangle_lua:
        byte_code = bytes(rectangle_lua.read(), 'utf-8')
    await dml.register_shared_class(class_name, byte_code, 'lua')
    java_class_def = await dml.get(class_name)
    assert java_class_def['className'] == class_name

    key = 'async_rect1'
    length, breadth = 10, 20
    await dml.create(key, class_name, 'lua', [length, breadth], None, True, True)
    area = float(await dml.invoke_method(key, 'getArea', None))
    assert abs(area - length * breadth) < 1e-06


@pytest.mark.asyncio
async def test_register_and_use_java(dml):
    class_name = 'SharedStatistics'
    with open('./resources/SharedStatistics.class', 'rb') as shared_statistics_java:
        byte_code = shared_statistics_java.read()
    await dml.register_shared_class(class_name, byte_code, 'java')
    lua_class_def = await dml.get(class_name)
    assert lua_class_def['className'] == class_name

    key = 'async_statistic1'
    value1, value2 = 3, 9
    await dml.create(key, class_name, 'java', None, None, True, True)
    is_added = bool(await dml.invoke_method(key, 'add', args=[value1]))
    assert is_added
    is_added = bool(await dml.invoke_method(key, 'add', args=[value2]))
    assert is_added
    average = float(await dml.invoke_method(key, 'average'))
    assert abs(average - (value1 + value2) / 2.0) < 1e-6


@pytest.mark.asyncio
async def test_set_get(dml):
    key = 'test_set_get'
    value = bytes(64)
    await dml.create(key)
    await dml.set(key, value)
    assert (await dml.get(key))['buffer'] == value


@pytest.mark.asyncio
async def test_lock_unlock(dml):
    key = 'test_lock_unlock'
    value = bytes(10)
    await dml.create(key)
    await dml.set(key, value)
    lock_token = await dml.lock(key)
    await dml.unlock(key, lock_token)
    assert (await dml.get(key))['buffer'] == value


@pytest.mark.asyncio
async def test_delete(dml):
    key = 'test_delete'
    await dml.create(key)
    await dml.delete(key)
    with pytest.raises(MetadataCommandException):
        assert await dml.get(key)

@pytest.mark.asyncio
async def test_get_zone_info(dml):
    zone_info = await dml.get_zone_info('local_zone1')
    assert 'weightedAverageUnitPrices' in zone_info
    assert 'totalFreeMemory' in zone_info
    print(zone_info)

@pytest.mark.asyncio
async def test_get_free_storage_nodes(dml):
    zones = ['local_zone1']
    storage_ids = await dml.get_free_storage_nodes(zones, 1024)
    assert storage_ids is not None
    assert len(storage_ids) > 0
    print(storage_ids)