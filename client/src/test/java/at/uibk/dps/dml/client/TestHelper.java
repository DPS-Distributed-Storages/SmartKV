package at.uibk.dps.dml.client;

import at.uibk.dps.dml.client.metadata.KeyConfiguration;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestHelper {

    public static DmlClient createDmlClient(Vertx vertx, VertxTestContext testContext) {
        DmlClient client = new DmlClient(vertx, "localRegion", "ap1", "localProvider");

        client.connect(TestConfig.METADATA_HOST, TestConfig.METADATA_PORT)
                .onComplete(testContext.succeedingThenComplete());

        return client;
    }

    @SuppressWarnings("rawtypes")
    public static Future<Void> deleteAllKeys(DmlClient client) {
        return client.getAllConfigurations()
                .compose(configs -> {
                    List<Future> futures = new ArrayList<>();
                    for (Map.Entry<String, KeyConfiguration> config : configs.entrySet()) {
                        futures.add(client.delete(config.getKey()));
                    }
                    return CompositeFuture.join(futures).mapEmpty();
                });
    }

    public static byte[] readBytesFromClass(Class clazz) {
        String path = clazz.getName().replace('.', File.separatorChar) + ".class";
        InputStream is = clazz.getClassLoader().getResourceAsStream(path);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int data = -1;
        while (true) {
            try {
                if (!((data = is.read()) != -1)) break;
            } catch (IOException e) {
                System.err.println("Could not read the class file: " + e.getMessage());
            }
            bos.write(data);

        }
        return bos.toByteArray();
    }

    public static byte[] readBytesFromFile(Path path){

        byte[] byteCode = null;

        try {
            byteCode = Files.readAllBytes(path);
        } catch (IOException e) {
            System.err.println("Could not read the class file: " + e.getMessage());
            e.printStackTrace();
        }

        return byteCode;

    }

}
