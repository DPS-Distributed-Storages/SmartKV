package at.uibk.dps.dml.client.metadata;

public class MetadataCommandError extends Throwable {

    private final MetadataCommandErrorType metadataCommandErrorType;

    public MetadataCommandError(MetadataCommandErrorType metadataCommandErrorType) {
        super("Command failed: " + metadataCommandErrorType);
        this.metadataCommandErrorType = metadataCommandErrorType;
    }

    public MetadataCommandErrorType getErrorType() {
        return metadataCommandErrorType;
    }
}
