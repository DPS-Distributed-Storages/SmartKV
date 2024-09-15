package at.uibk.dps.dml.node.util;

import at.uibk.dps.dml.client.storage.BsonArgsCodec;
import at.uibk.dps.dml.node.storage.CustomClassLoader;
import at.uibk.dps.dml.node.storage.SharedJavaObjectWrapper;
import at.uibk.dps.dml.node.storage.StorageObject;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class JOLUtilTest {

    @Test
    void testGetObjectSize() {
        Map<String, StorageObject> objects = new HashMap<>();
        BsonArgsCodec argsCodec = new BsonArgsCodec();
        CustomClassLoader classLoader = new CustomClassLoader();

        long sizeBefore = MemoryUtil.getDeepObjectSize(objects);

        for(int i = 0; i < 500; i++){
            StorageObject storageObject = new StorageObject(new Timestamp(0,0));
            storageObject.setSharedObject(new SharedJavaObjectWrapper(argsCodec, "SharedCounter", argsCodec.encode(new Object[]{i}), classLoader));
            objects.put(Integer.toString(i), storageObject);
        }

        long sizeAfterPuts = MemoryUtil.getDeepObjectSize(objects);
        assertTrue(sizeAfterPuts > 0);
        assertTrue(sizeAfterPuts > sizeBefore);

        for(int i = 500; i < 1000; i++){
            byte[] byteArray = "some random text".repeat(i/10).getBytes();
            StorageObject storageObject = new StorageObject(new Timestamp(0,0));
            storageObject.setSharedObject(new SharedJavaObjectWrapper(argsCodec, "SharedBuffer", argsCodec.encode(new Object[]{byteArray}), classLoader));
            objects.put(Integer.toString(i), storageObject);
        }

        long sizeAfterPuts2 = MemoryUtil.getDeepObjectSize(objects);

        assertTrue(sizeAfterPuts2 > sizeAfterPuts);

        objects.remove("1");

        long sizeAfterRemove = MemoryUtil.getDeepObjectSize(objects);

        assertTrue(sizeAfterRemove < sizeAfterPuts2);
    }

}
