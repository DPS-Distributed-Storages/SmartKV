package at.uibk.dps.dml.node.membership;

import com.fasterxml.jackson.annotation.JsonManagedReference;

import java.util.List;
import java.util.Set;

public class DmlNodeInfo {

    private String region = "local";

    private String provider = "local";

    private String zone = "local";

    private String hostname = "localhost";

    private int defaultNumReplicas = 1;

    private boolean allowReplicasOnTheSameNode = false;

    // memory in bytes, default 1 GB
    private int memory = 1024 * 1024 * 1024;

    private DmlNodeUnitPrices unitPrices;

    @JsonManagedReference
    private Set<VerticleInfo> verticles;

    public DmlNodeInfo() {
        unitPrices = new DmlNodeUnitPrices();
    }

    public DmlNodeInfo(String region,  String zone, String provider, String hostname, int defaultNumReplicas,
                       boolean allowReplicasOnTheSameNode, DmlNodeUnitPrices unitPrices, Set<VerticleInfo> verticles) {
        this(region, zone, provider, hostname, 1024 * 1024 * 1024, defaultNumReplicas, allowReplicasOnTheSameNode, unitPrices, verticles);
    }

    public DmlNodeInfo(String region,  String zone, String provider, String hostname, int memory, int defaultNumReplicas,
                       boolean allowReplicasOnTheSameNode, DmlNodeUnitPrices unitPrices, Set<VerticleInfo> verticles) {
        this.region = region;
        this.zone = zone;
        this.provider = provider;
        this.hostname = hostname;
        this.memory = memory;
        this.defaultNumReplicas = defaultNumReplicas;
        this.allowReplicasOnTheSameNode = allowReplicasOnTheSameNode;
        this.unitPrices = unitPrices;
        this.verticles = verticles;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getDefaultNumReplicas() {
        return defaultNumReplicas;
    }

    public void setDefaultNumReplicas(int defaultNumReplicas) {
        this.defaultNumReplicas = defaultNumReplicas;
    }

    public boolean isAllowReplicasOnTheSameNode() {
        return allowReplicasOnTheSameNode;
    }

    public void setAllowReplicasOnTheSameNode(boolean allowReplicasOnTheSameNode) {
        this.allowReplicasOnTheSameNode = allowReplicasOnTheSameNode;
    }

    public DmlNodeUnitPrices getUnitPrices() {
        return unitPrices;
    }

    public void setUnitPrices(DmlNodeUnitPrices unitPrices) {
        this.unitPrices = unitPrices;
    }

    public Set<VerticleInfo> getVerticles() {
        return verticles;
    }

    public void setVerticles(Set<VerticleInfo> verticles) {
        this.verticles = verticles;
    }

    public int getMemory() {
        return memory;
    }

    public void setMemory(int memory) {
        this.memory = memory;
    }
}
