package at.uibk.dps.dml.cli.commands;

import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.client.DmlClient;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.*;

import java.util.LinkedHashSet;
import java.util.List;

@Name("reconfigure")
public class ReconfigureCommand extends Command<DmlClient> {

    private String key;

    private List<Integer> newReplicas;

    public ReconfigureCommand(CLI cli) {
        super(cli);
    }

    @Argument(index = 0, argName = "key")
    public void setKey(String key) {
        this.key = key;
    }

    // We use @Option because @ParsedAsList somehow does not work with @Argument
    @Option(argName = "new-replicas", longName = "replicas", shortName = "r", required = true)
    @Description("The verticle IDs storing replicas. Multiple verticle IDs need to be separated by a comma.")
    @ParsedAsList
    public void setNewReplicas(List<Integer> newReplicas) {
        this.newReplicas = newReplicas;
    }

    @Override
    @SuppressWarnings("java:S106")
    public void execute(CommandLine commandLine, DmlClient client) {
        client.reconfigure(key, !newReplicas.isEmpty() ? new LinkedHashSet<>(newReplicas) : null)
                .onSuccess(res -> System.out.println(getCli().getName() + " successful"))
                .onFailure(err -> System.err.println(err.getMessage()));
    }
}
