package at.uibk.dps.dml.node.storage.command;

/**
 * The {@link CommandHandler} interface is implemented by classes which handle the different types of commands.
 */
public interface CommandHandler {

    /**
     * Applies the given {@link StateReplicationCommand}.
     *
     * @param command the command
     * @return the command result
     */
    Object apply(StateReplicationCommand command);

    /**
     * Applies the given {@link LockCommand}.
     *
     * @param command the command
     * @return the command result
     */
    Object apply(LockCommand command);

    /**
     * Applies the given {@link UnlockCommand}.
     *
     * @param command the command
     * @return the command result
     */
    Object apply(UnlockCommand command);

    /**
     * Applies the given {@link InitObjectCommand}.
     *
     * @param command the command
     * @return the command result
     */
    Object apply(InitObjectCommand command);

    /**
     * Applies the given {@link InvokeMethodCommand}.
     *
     * @param command the command
     * @return the command result
     */
    Object apply(InvokeMethodCommand command);


    /**
     * Applies the given {@link PushClientLocationCommand}.
     *
     * @param command the command
     * @return the command result
     */
    Object apply(PushClientLocationCommand command);


    /**
     * Applies the given {@link SetCommand}.
     *
     * @param command the command
     * @return the command result
     */
    Object apply(SetCommand command);


    /**
     * Applies the given {@link GetCommand}.
     *
     * @param command the command
     * @return the command result
     */
    Object apply(GetCommand command);
}
