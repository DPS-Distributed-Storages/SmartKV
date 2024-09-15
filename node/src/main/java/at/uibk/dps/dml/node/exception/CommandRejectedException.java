package at.uibk.dps.dml.node.exception;

public class CommandRejectedException extends RuntimeException {

    public CommandRejectedException() {
        super();
    }

    public CommandRejectedException(String message) {
        super(message);
    }

}
