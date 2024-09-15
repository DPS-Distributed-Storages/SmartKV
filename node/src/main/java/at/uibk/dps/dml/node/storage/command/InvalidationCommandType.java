package at.uibk.dps.dml.node.storage.command;

/**
 * The {@link InvalidationCommandType} enum contains the types of requests which are to be replicated.
 * The ordinal of each enum value is used to encode the command type in the RPC message.
 */
public enum InvalidationCommandType {

    STATE_REPLICATION,

    LOCK,

    UNLOCK,

    INIT_OBJECT,

    INVOKE_METHOD,

    SET,

    GET

}
