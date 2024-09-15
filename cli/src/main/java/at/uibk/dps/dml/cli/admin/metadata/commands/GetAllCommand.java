package at.uibk.dps.dml.cli.admin.metadata.commands;

import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.client.metadata.MetadataClient;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.Name;

@Name("getAll")
public class GetAllCommand extends Command<MetadataClient> {

    public GetAllCommand(CLI cli) {
        super(cli);
    }

    @Override
    @SuppressWarnings("java:S106")
    public void execute(CommandLine commandLine, MetadataClient client) {
        client.getAll()
                .onSuccess(configurations -> System.out.println(configurations == null ? "null" : configurations))
                .onFailure(err -> System.err.println(err.getMessage()));
    }
}
