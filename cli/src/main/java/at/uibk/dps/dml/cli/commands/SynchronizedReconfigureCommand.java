package at.uibk.dps.dml.cli.commands;

import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.client.DmlClient;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.*;

import java.util.LinkedHashSet;
import java.util.List;

@Name("synchronizedReconfigure")
public class SynchronizedReconfigureCommand extends Command<DmlClient> {

    private String key;

    private List<Integer> oldReplicas;

    private List<Integer> newReplicas;

    public SynchronizedReconfigureCommand(CLI cli) {
        super(cli);
    }

    @Argument(index = 0, argName = "key")
    public void setKey(String key) {
        this.key = key;
    }

    // We use @Option because @ParsedAsList somehow does not work with @Argument
    @Option(argName = "old-replicas", longName = "old_replicas", shortName = "or", required = true)
    @Description("The old verticle IDs storing replicas. Multiple verticle IDs need to be separated by a comma.")
    @ParsedAsList
    public void setOldReplicas(List<Integer> oldReplicas) {
        this.oldReplicas = oldReplicas;
    }

    @Option(argName = "new-replicas", longName = "new_replicas", shortName = "nr", required = true)
    @Description("The new verticle IDs storing replicas. Multiple verticle IDs need to be separated by a comma.")
    @ParsedAsList
    public void setNewReplicas(List<Integer> newReplicas) {
        this.newReplicas = newReplicas;
    }

    @Override
    @SuppressWarnings("java:S106")
    public void execute(CommandLine commandLine, DmlClient client) {
        client.synchronizedReconfigure(key, !oldReplicas.isEmpty() ? new LinkedHashSet<>(oldReplicas) : null, !newReplicas.isEmpty() ? new LinkedHashSet<>(newReplicas) : null)
                .onSuccess(res -> System.out.println(getCli().getName() + " successful"))
                .onFailure(err -> System.err.println(err.getMessage()));
    }
}
