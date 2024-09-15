package at.uibk.dps.dml.cli.commands;

import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.client.DmlClient;
import at.uibk.dps.dml.client.metadata.KeyConfiguration;
import at.uibk.dps.dml.client.metadata.Storage;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.Name;

import java.util.List;
import java.util.Map;

@Name("getAll")
public class GetAllConfigurationsCommand extends Command<DmlClient> {

    public GetAllConfigurationsCommand(CLI cli) {
        super(cli);
    }

    @Override
    @SuppressWarnings("java:S106")
    public void execute(CommandLine commandLine, DmlClient client) {
        client.getAllConfigurations()
                .onSuccess(value -> {
                    if (value.isEmpty()) {
                        System.out.println("Metadata server is empty");
                        return;
                    }
                    for (Map.Entry<String, KeyConfiguration> entry : value.entrySet()) {
                        System.out.println("Retrieved key: " + entry.getKey());
                        System.out.println("Metadata version: " + entry.getValue().getVersion());
                        List<Storage> replicas = entry.getValue().getReplicas();
                        for (Storage storage : replicas) {
                            System.out.println("> Storage: " + storage);
                        }
                    }
                })
                .onFailure(err -> System.err.println(err.getMessage()));

    }
}
