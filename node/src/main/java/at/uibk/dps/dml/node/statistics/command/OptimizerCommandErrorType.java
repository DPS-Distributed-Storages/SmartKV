package at.uibk.dps.dml.node.statistics.command;
import java.util.Arrays;

public enum OptimizerCommandErrorType {

        UNKNOWN_ERROR(0, "An unknown error occurred"),

        UNKNOWN_COMMAND(1, "Unknown command");

        private final int errorCode;

        private final String message;

    OptimizerCommandErrorType(int errorCode, String message) {
            this.errorCode = errorCode;
            this.message = message;
        }

        public int getErrorCode() {
            return errorCode;
        }

        public static OptimizerCommandErrorType valueOf(int errorCode) {
            return Arrays.stream(OptimizerCommandErrorType.values())
                    .filter(value -> value.errorCode == errorCode)
                    .findFirst()
                    .orElse(null);
        }

        @Override
        public String toString() {
            return "OptimizerCommandErrorType{" +
                    "errorCode=" + errorCode +
                    ", message='" + message + '\'' +
                    '}';
        }
    }
