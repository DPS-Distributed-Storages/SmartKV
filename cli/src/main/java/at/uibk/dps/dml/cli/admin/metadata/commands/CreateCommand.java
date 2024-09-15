package at.uibk.dps.dml.cli.admin.metadata.commands;

import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.client.metadata.MetadataClient;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.*;

import java.util.LinkedHashSet;
import java.util.List;

@Name("create")
public class CreateCommand extends Command<MetadataClient> {

    private String key;

    private List<Integer> replicas;

    public CreateCommand(CLI cli) {
        super(cli);
    }

    @Argument(index = 0, argName = "key")
    public void setKey(String key) {
        this.key = key;
    }

    @Option(argName = "replicas", longName = "replicas", shortName = "r")
    @Description("The verticle IDs storing replicas. Multiple verticle IDs need to be separated by a comma.")
    @ParsedAsList
    public void setReplicas(List<Integer> replicas) {
        this.replicas = replicas;
    }

    @Override
    @SuppressWarnings("java:S106")
    public void execute(CommandLine commandLine, MetadataClient client) {
        client.create(key, !replicas.isEmpty() ? new LinkedHashSet<>(replicas) : null)
                .onSuccess(res -> System.out.println(getCli().getName() + " successful"))
                .onFailure(err -> System.err.println(err.getMessage()));
    }
}
