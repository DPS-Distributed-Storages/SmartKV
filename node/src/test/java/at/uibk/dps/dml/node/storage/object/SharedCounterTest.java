package at.uibk.dps.dml.node.storage.object;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SharedCounterTest {

    @Test
    void testEmptyConstructor() {
        SharedCounter counter = new SharedCounter();
        assertEquals(0, counter.get());
    }

    @Test
    void testConstructorWithInitialValue() {
        long initial = 5;

        SharedCounter counter = new SharedCounter(initial);

        assertEquals(initial, counter.get());
    }

    @Test
    void testIncrement() {
        SharedCounter counter = new SharedCounter();
        assertEquals(1, counter.increment());
        assertEquals(1, counter.get());
        assertEquals(2, counter.increment(1));
        assertEquals(2, counter.get());
        assertEquals(4, counter.increment(2));
        assertEquals(4, counter.get());
    }

    @Test
    void testDecrement() {
        SharedCounter counter = new SharedCounter(10);
        assertEquals(9, counter.decrement());
        assertEquals(9, counter.get());
        assertEquals(5, counter.decrement(4));
        assertEquals(5, counter.get());
        assertEquals(0, counter.decrement(5));
        assertEquals(0, counter.get());
        assertEquals(-1, counter.decrement());
        assertEquals(-1, counter.get());
    }
}
