package at.uibk.dps.dml.node.util;

public class NodeLocation {

    private final String region;

    // zone for storage nodes / access point for clients!
    private final String zone;

    private final String provider;

    public NodeLocation(String region, String zone, String provider) {
        this.region = region;
        this.zone = zone;
        this.provider = provider;
    }

    public String getRegion() {
        return region;
    }

    public String getProvider() {
        return provider;
    }

    public String getZone(){
        return zone;
    }

    @Override
    public boolean equals(Object o) {

        if (o == this) {
            return true;
        }

        if (!(o instanceof NodeLocation)) {
            return false;
        }

        NodeLocation otherNodeLocation = (NodeLocation) o;

        return otherNodeLocation.getProvider().equals(this.getProvider()) && otherNodeLocation.getRegion().equals(this.getRegion()) && otherNodeLocation.getZone().equals(this.getZone());
    }

}
