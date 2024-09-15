package at.uibk.dps.dml.node.metadata.command;

import io.vertx.core.Promise;

public class GetCommand extends Command {

    public GetCommand(Promise<Object> promise, String key) {
        super(promise, key, true, false);
    }

    @Override
    public Object apply(CommandHandler handler) {
        return handler.apply(this);
    }
}
