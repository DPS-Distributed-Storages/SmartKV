package at.uibk.dps.dml.node.statistics.command;

import io.vertx.core.Promise;

public class GetStatisticsCommand extends Command {

    public GetStatisticsCommand(Promise<Object> promise) {
        super(promise);
    }

    @Override
    public Object apply(CommandHandler handler) {
        return handler.apply(this);
    }
}
