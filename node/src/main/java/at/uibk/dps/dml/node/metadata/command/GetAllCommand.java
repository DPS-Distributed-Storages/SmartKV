package at.uibk.dps.dml.node.metadata.command;

import io.vertx.core.Promise;

public class GetAllCommand extends Command {

    public GetAllCommand(Promise<Object> promise) {
        super(promise, null, true, true);
    }

    @Override
    public Object apply(CommandHandler handler) {
        return handler.apply(this);
    }
}
