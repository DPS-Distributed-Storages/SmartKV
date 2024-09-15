package at.uibk.dps.dml.cli.admin.storage;

import at.uibk.dps.dml.cli.BaseCli;
import at.uibk.dps.dml.cli.admin.storage.commands.*;
import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.client.storage.BsonArgsCodec;
import at.uibk.dps.dml.client.storage.StorageClient;
import io.vertx.core.Vertx;

import java.util.*;

public class StorageCli extends BaseCli {

    private static final List<Class<? extends Command<StorageClient>>> commandClasses = Arrays.asList(
            LockCommand.class,
            UnlockCommand.class,
            InitObjectCommand.class,
            InvokeMethodCommand.class
    );

    @SuppressWarnings("java:S106")
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        String hostname;
        int port;
        if (args.length == 2) {
            hostname = args[0];
            port = Integer.parseInt(args[1]);
        } else {
            Scanner in = new Scanner(System.in);
            System.out.print("Enter hostname of the storage server: ");
            hostname = in.nextLine();
            System.out.print("Enter port of the storage server: ");
            port = in.nextInt();
        }

        StorageClient client = new StorageClient(vertx, new BsonArgsCodec());
        client.connect(hostname, port)
                .onSuccess(res -> System.out.println("Connected successfully"))
                .onFailure(err -> System.err.println("Failed to connect"));

        start(client, commandClasses);

        client.disconnect().onComplete(res -> {
            vertx.close();
            if (res.failed()) {
                System.out.println(res.cause().getMessage());
            }
        });
    }
}
