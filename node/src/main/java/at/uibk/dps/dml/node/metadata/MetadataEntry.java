package at.uibk.dps.dml.node.metadata;

import at.uibk.dps.dml.node.util.Timestamp;

public class MetadataEntry {

    private Timestamp timestamp;

    private MetadataEntryState state;
    private KeyMetadata metadata;
    private KeyMetadata oldMetadata;

    public MetadataEntry(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public MetadataEntryState getState() {
        return state;
    }

    public void setState(MetadataEntryState state) {
        this.state = state;
    }

    public KeyMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(KeyMetadata metadata) {
        this.metadata = metadata;
    }

    public KeyMetadata getOldMetadata() {
        return oldMetadata;
    }

    public void setOldMetadata(KeyMetadata oldMetadata) {
        this.oldMetadata = oldMetadata;
    }
}
