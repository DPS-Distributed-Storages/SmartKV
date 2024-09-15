package at.uibk.dps.dml.node.statistics.command;

import at.uibk.dps.dml.node.statistics.command.CommandHandler;
import io.vertx.core.Promise;

public abstract class Command {

    protected final Promise<Object> promise;

    protected Command() {
        this.promise = null;
    }

    protected Command(Promise<Object> promise) {
        this.promise = promise;
    }

    public Promise<Object> getPromise() {
        return promise;
    }

    public abstract Object apply(CommandHandler handler);

}
