package at.uibk.dps.dml.client;

public class TestConfig {

    public static final String METADATA_HOST = System.getProperty("dml.hostname", "localhost");
    public static final int METADATA_PORT = Integer.getInteger("dml.port", 9000);

}
