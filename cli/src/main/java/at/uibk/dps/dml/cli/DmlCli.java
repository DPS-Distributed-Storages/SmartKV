package at.uibk.dps.dml.cli;

import at.uibk.dps.dml.cli.commands.*;
import at.uibk.dps.dml.client.DmlClient;
import at.uibk.dps.dml.client.storage.BsonArgsCodec;
import at.uibk.dps.dml.client.storage.SimpleStorageSelector;
import io.vertx.core.Vertx;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class DmlCli extends BaseCli {

    private static final List<Class<? extends Command<DmlClient>>> commandClasses = Arrays.asList(
            CreateCommand.class,
            RegisterClassCommand.class,
            DeleteCommand.class,
            GetCommand.class,
            GetAllConfigurationsCommand.class,
            GetMembershipViewCommand.class,
            InvokeMethodCommand.class,
            LockCommand.class,
            ReconfigureCommand.class,
            SynchronizedReconfigureCommand.class,
            SetCommand.class,
            UnlockCommand.class
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
            System.out.print("Enter hostname of a metadata server: ");
            hostname = in.nextLine();
            System.out.print("Enter port of the metadata server: ");
            port = in.nextInt();
        }

        DmlClient client = new DmlClient(vertx, "local_region", "e3_s2", "local_provider", new SimpleStorageSelector(), new BsonArgsCodec());
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
