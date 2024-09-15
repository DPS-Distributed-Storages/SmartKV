package at.uibk.dps.dml.node.metadata.command;

public interface CommandHandler {

    Object apply(CreateCommand command);

    Object apply(GetCommand command);

    Object apply(GetAllCommand command);

    Object apply(ReconfigureCommand command);

    Object apply(SynchronizedReconfigureCommand synchronizedReconfigureCommand);

    Object apply(DeleteCommand command);

}
