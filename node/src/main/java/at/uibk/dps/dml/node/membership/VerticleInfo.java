package at.uibk.dps.dml.node.membership;

import com.fasterxml.jackson.annotation.JsonBackReference;

public class VerticleInfo {

    private int id;

    private VerticleType type;

    private int port;

    @JsonBackReference
    private DmlNodeInfo ownerNode;

    public VerticleInfo() {
    }

    public VerticleInfo(int id, VerticleType type, int port, DmlNodeInfo ownerNode) {
        this.id = id;
        this.type = type;
        this.port = port;
        this.ownerNode = ownerNode;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public VerticleType getType() {
        return type;
    }

    public void setType(VerticleType type) {
        this.type = type;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public DmlNodeInfo getOwnerNode() {
        return ownerNode;
    }

    public void setOwnerNode(DmlNodeInfo ownerNode) {
        this.ownerNode = ownerNode;
    }
}
