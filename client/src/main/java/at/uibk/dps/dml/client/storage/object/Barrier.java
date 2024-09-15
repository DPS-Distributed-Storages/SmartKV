package at.uibk.dps.dml.client.storage.object;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.concurrent.TimeoutException;

public class Barrier {

    private static final int DEFAULT_CHECK_DELAY_MS = 100;

    private final Vertx vertx;
    private final int parties;
    private final SharedCounter counter;
    private final int checkDelayMs;

    public Barrier(Vertx vertx, int parties, SharedCounter counter) {
        this.vertx = vertx;
        this.parties = parties;
        this.counter = counter;
        this.checkDelayMs = DEFAULT_CHECK_DELAY_MS;
    }

    public Barrier(Vertx vertx, int parties, SharedCounter counter, int checkDelayMs) {
        this.vertx = vertx;
        this.parties = parties;
        this.counter = counter;
        this.checkDelayMs = checkDelayMs;
    }

    /**
     * Waits until all parties have called {@code await} on this barrier.
     *
     * @return a future that completes when all parties have called @{code await}
     */
    public Future<Void> await() {
        return await(false, 0L, 0L);
    }

    /**
     * Same as {@link #await()} but with a timeout.
     *
     * @param timeoutMs the timeout in milliseconds
     * @return a future that succeeds when all parties have called {@code await} or fails with a
     * {@link TimeoutException} if the specified timeout is reached
     */
    public Future<Void> await(long timeoutMs) {
        return await(true, System.currentTimeMillis(), timeoutMs);
    }

    private Future<Void> await(boolean checkTimeout, long startTime, long timeoutMs) {
        Promise<Void> promise = Promise.promise();
        counter.increment()
                .onSuccess(res -> doWait(promise, checkTimeout, startTime, timeoutMs))
                .onFailure(promise::fail);
        return promise.future();
    }

    private void doWait(Promise<Void> promise, boolean checkTimeout, long startTime, long timeoutMs) {
        counter.get().onSuccess(counterValue -> {
            if (counterValue % parties == 0) {
                promise.complete();
            } else if (checkTimeout && System.currentTimeMillis() - startTime > timeoutMs) {
                promise.fail(new TimeoutException());
            } else {
                vertx.setTimer(checkDelayMs, id -> doWait(promise, checkTimeout, startTime, timeoutMs));
            }
        }).onFailure(promise::fail);
    }
}
