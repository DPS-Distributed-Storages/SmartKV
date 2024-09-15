package at.uibk.dps.dml.client.storage;

public class Response<T> {

    private final int metadataVersion;
    private final T result;

    public Response(int metadataVersion, T result) {
        this.metadataVersion = metadataVersion;
        this.result = result;
    }

    public int getMetadataVersion() {
        return metadataVersion;
    }

    public T getResult() {
        return result;
    }
}
