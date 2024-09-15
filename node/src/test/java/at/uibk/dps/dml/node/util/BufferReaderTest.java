package at.uibk.dps.dml.node.util;

import io.vertx.core.buffer.Buffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BufferReaderTest {

    @Test
    void testGetInt() {
        Buffer buffer = Buffer.buffer();
        buffer.appendInt(11);
        buffer.appendInt(12);
        BufferReader bufferReader = new BufferReader(buffer);
        assertEquals(11, bufferReader.getInt());
        assertEquals(12, bufferReader.getInt());
        assertTrue(bufferReader.reachedEnd());
    }

    @Test
    void testGetLong() {
        Buffer buffer = Buffer.buffer();
        buffer.appendLong(21L);
        buffer.appendLong(22L);
        buffer.appendLong(23L);
        BufferReader bufferReader = new BufferReader(buffer);
        assertEquals(21L, bufferReader.getLong());
        assertEquals(22L, bufferReader.getLong());
        assertEquals(23L, bufferReader.getLong());
        assertTrue(bufferReader.reachedEnd());
    }

    @Test
    void testGetByte() {
        Buffer buffer = Buffer.buffer();
        buffer.appendByte((byte) 2);
        buffer.appendByte((byte) 3);
        BufferReader bufferReader = new BufferReader(buffer);
        assertEquals(2, bufferReader.getByte());
        assertEquals(3, bufferReader.getByte());
        assertTrue(bufferReader.reachedEnd());
    }

    @Test
    void testGetBytes() {
        Buffer buffer = Buffer.buffer();
        byte[] arr1 = new byte[]{1, 2, 3, 4, 5, 6};
        byte[] arr2 = new byte[]{1};
        byte[] arr3 = new byte[]{};
        buffer.appendBytes(arr1);
        buffer.appendBytes(arr2);
        buffer.appendBytes(arr3);
        BufferReader bufferReader = new BufferReader(buffer);
        assertArrayEquals(arr1, bufferReader.getBytes(arr1.length));
        assertArrayEquals(arr2, bufferReader.getBytes(arr2.length));
        assertArrayEquals(arr3, bufferReader.getBytes(arr3.length));
        assertTrue(bufferReader.reachedEnd());
    }

    @Test
    void testGetLengthPrefixedBytes() {
        Buffer buffer = Buffer.buffer();
        byte[] arr1 = new byte[]{2, 3, 4, 5, 6};
        byte[] arr2 = new byte[]{3,2,1};
        buffer.appendInt(arr1.length);
        buffer.appendBytes(arr1);
        buffer.appendInt(arr2.length);
        buffer.appendBytes(arr2);
        buffer.appendInt(-1);
        BufferReader bufferReader = new BufferReader(buffer);
        assertArrayEquals(arr1, bufferReader.getLengthPrefixedBytes());
        assertArrayEquals(arr2, bufferReader.getLengthPrefixedBytes());
        assertNull(bufferReader.getLengthPrefixedBytes());
        assertTrue(bufferReader.reachedEnd());
    }

    @Test
    void testGetString() {
        Buffer buffer = Buffer.buffer();
        String str1 = "abc";
        String str2 = "def";
        String str3 = "";
        buffer.appendString(str1);
        buffer.appendString(str2);
        buffer.appendString(str3);
        BufferReader bufferReader = new BufferReader(buffer);
        assertEquals(str1, bufferReader.getString(str1.length()));
        assertEquals(str2, bufferReader.getString(str2.length()));
        assertEquals("", bufferReader.getString(0));
        assertTrue(bufferReader.reachedEnd());
    }

    @Test
    void testGetLengthPrefixedString() {
        Buffer buffer = Buffer.buffer();
        String str1 = "test1";
        String str2 = "";
        String str3 = "test2";
        buffer.appendInt(str1.length());
        buffer.appendString(str1);
        buffer.appendInt(0);
        buffer.appendString(str2);
        buffer.appendInt(str3.length());
        buffer.appendString(str3);
        buffer.appendInt(-1);
        BufferReader bufferReader = new BufferReader(buffer);
        assertEquals(str1, bufferReader.getLengthPrefixedString());
        assertEquals(str2, bufferReader.getLengthPrefixedString());
        assertEquals(str3, bufferReader.getLengthPrefixedString());
        assertNull(bufferReader.getLengthPrefixedString());
        assertTrue(bufferReader.reachedEnd());
    }
}
