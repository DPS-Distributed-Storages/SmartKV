package at.uibk.dps.dml.node.statistics.command;

import io.vertx.core.Promise;

public class ClearStatisticsCommand extends Command {

    public ClearStatisticsCommand(Promise<Object> promise) {
        super(promise);
    }

    @Override
    public Object apply(CommandHandler handler) {
        return handler.apply(this);
    }
}
