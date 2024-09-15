package at.uibk.dps.dml.cli.admin.metadata;

import at.uibk.dps.dml.cli.BaseCli;
import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.cli.admin.metadata.commands.*;
import at.uibk.dps.dml.client.metadata.MetadataClient;
import io.vertx.core.Vertx;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class MetadataCli extends BaseCli {

    private static final List<Class<? extends Command<MetadataClient>>> commandClasses = Arrays.asList(
            CreateCommand.class,
            DeleteCommand.class,
            GetCommand.class,
            GetAllCommand.class,
            GetMembershipViewCommand.class,
            ReconfigureCommand.class
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
            System.out.print("Enter hostname of the metadata server: ");
            hostname = in.nextLine();
            System.out.print("Enter port of the metadata server: ");
            port = in.nextInt();
        }

        MetadataClient client = new MetadataClient(vertx);
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
