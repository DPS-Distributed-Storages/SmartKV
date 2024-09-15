package at.uibk.dps.dml.cli.admin.storage.commands;

import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.client.storage.StorageClient;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.Argument;
import io.vertx.core.cli.annotations.Name;

@Name("lock")
public class LockCommand extends Command<StorageClient> {

    private String key;

    public LockCommand(CLI cli) {
        super(cli);
    }

    @Argument(index = 0, argName = "key")
    public void setKey(String key) {
        this.key = key;
    }

    @Override
    @SuppressWarnings("java:S106")
    public void execute(CommandLine commandLine, StorageClient client) {
        client.lock(key)
                .onSuccess(response ->
                        System.out.println(getCli().getName() + " successful -> lock token: " + response.getResult()))
                .onFailure(err -> System.err.println(err.getMessage()));
    }
}
