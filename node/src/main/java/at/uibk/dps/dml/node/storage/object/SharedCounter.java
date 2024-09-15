package at.uibk.dps.dml.node.storage.object;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;

/**
 * The {@link SharedCounter} holds a shared counter.
 */
public class SharedCounter implements Serializable {

    private long value;

    /**
     * Default constructor that initializes the counter with 0.
     */
    public SharedCounter() {
    }

    /**
     * Constructs a counter with the given initial value.
     *
     * @param value the value
     */
    public SharedCounter(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    /**
     * Returns the current value of the counter.
     *
     * @return the current value of the counter
     */
    public long get() {
        return value;
    }

    /**
     * Sets the counter to the given value.
     */
    public void set(long value) {
        this.value = value;
    }

    /**
     * Increments the counter by 1.
     *
     * @return the new value of the counter
     */
    public long increment() {
        return increment(1);
    }

    /**
     * Increments the counter by the given delta and returns the new value.
     *
     * @param delta the delta by which the counter is incremented
     * @return the new value of the counter
     */
    public long increment(long delta) {
        value += delta;
        return value;
    }

    /**
     * Decrements the counter by 1.
     *
     * @return the new value of the counter
     */
    public long decrement() {
        return decrement(1);
    }

    /**
     * Decrements the counter by the given delta and returns the new value.
     *
     * @param delta the delta by which the counter is decremented
     * @return the new value of the counter
     */
    public long decrement(long delta) {
        value -= delta;
        return value;
    }
}
