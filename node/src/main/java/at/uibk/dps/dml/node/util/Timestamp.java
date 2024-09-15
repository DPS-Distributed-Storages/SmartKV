package at.uibk.dps.dml.node.util;

import java.io.Serializable;
import java.util.Objects;

public class Timestamp implements Comparable<Timestamp>, Serializable {

    private long version;

    private int coordinatorVerticleId;

    public Timestamp(long version, int coordinatorVerticleId) {
        this.version = version;
        this.coordinatorVerticleId = coordinatorVerticleId;
    }

    public Timestamp(Timestamp timestamp) {
        this(timestamp.getVersion(), timestamp.getCoordinatorVerticleId());
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public int getCoordinatorVerticleId() {
        return coordinatorVerticleId;
    }

    public void setCoordinatorVerticleId(int coordinatorVerticleId) {
        this.coordinatorVerticleId = coordinatorVerticleId;
    }

    public boolean isLessThan(Timestamp otherTimestamp) {
        return compareTo(otherTimestamp) < 0;
    }

    public boolean isGreaterThan(Timestamp otherTimestamp) {
        return compareTo(otherTimestamp) > 0;
    }

    @Override
    public int compareTo(Timestamp otherTimestamp) {
        if (this.version > otherTimestamp.version
                || (this.version == otherTimestamp.version && this.coordinatorVerticleId > otherTimestamp.coordinatorVerticleId)) {
            return 1;
        }
        if (this.version < otherTimestamp.version || this.coordinatorVerticleId < otherTimestamp.coordinatorVerticleId) {
            return -1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Timestamp timestamp = (Timestamp) o;
        return version == timestamp.version && coordinatorVerticleId == timestamp.coordinatorVerticleId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, coordinatorVerticleId);
    }

    @Override
    public String toString() {
        return "Timestamp{" +
                "version=" + version +
                ", coordinatorVerticleId=" + coordinatorVerticleId +
                '}';
    }
}
