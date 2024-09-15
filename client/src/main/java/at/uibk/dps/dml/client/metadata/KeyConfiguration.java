package at.uibk.dps.dml.client.metadata;

import java.util.List;
import java.util.Objects;

public class KeyConfiguration {

    private int version;
    private List<Storage> replicas;

    public KeyConfiguration(int version, List<Storage> replicas) {
        this.version = version;
        this.replicas = replicas;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public List<Storage> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<Storage> replicas) {
        this.replicas = replicas;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyConfiguration that = (KeyConfiguration) o;
        return version == that.version && Objects.equals(replicas, that.replicas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, replicas);
    }

    @Override
    public String toString() {
        return "KeyConfiguration{" +
                "version=" + version +
                ", storages=" + replicas +
                '}';
    }
}
