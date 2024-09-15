package at.uibk.dps.dml.client;

public class NodeLocation {

    private final String region;

    private final String accessPoint;

    private final String provider;

    public NodeLocation(String region, String accessPoint, String provider) {
        this.region = region;
        this.accessPoint = accessPoint;
        this.provider = provider;
    }

    public String getRegion() {
        return region;
    }

    public String getProvider() {
        return provider;
    }

    public String getAccessPoint() {
        return accessPoint;
    }

}
