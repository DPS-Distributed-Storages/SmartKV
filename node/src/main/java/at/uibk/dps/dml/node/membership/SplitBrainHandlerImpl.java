package at.uibk.dps.dml.node.membership;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class SplitBrainHandlerImpl implements SplitBrainHandler {

    private final Logger logger = LoggerFactory.getLogger(SplitBrainHandlerImpl.class);

    private final Vertx vertx;

    public SplitBrainHandlerImpl(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void handle() {
        logger.warn("Potential split-brain scenario detected. Terminating node.");
        CountDownLatch latch = new CountDownLatch(1);
        vertx.close().onComplete(res -> latch.countDown());
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.exit(0);
    }
}
