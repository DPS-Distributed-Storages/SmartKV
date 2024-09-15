package at.uibk.dps.dml.client.metadata;

import java.util.Objects;

public class Storage {

    private int id;

    private String region;

    private String hostname;

    private int port;

    public Storage() {
    }

    public Storage(int id, String region, String hostname, int port) {
        this.id = id;
        this.region = region;
        this.hostname = hostname;
        this.port = port;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Storage storage = (Storage) o;
        return id == storage.id && port == storage.port && Objects.equals(region, storage.region) && Objects.equals(hostname, storage.hostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, region, hostname, port);
    }

    @Override
    public String toString() {
        return "Storage{" +
                "id=" + id +
                ", region='" + region + '\'' +
                ", hostname='" + hostname + '\'' +
                ", port=" + port +
                '}';
    }
}
