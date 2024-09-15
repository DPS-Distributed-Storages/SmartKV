package at.uibk.dps.dml.cli.admin.storage.commands;

import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.client.storage.StorageClient;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.Argument;
import io.vertx.core.cli.annotations.Name;

@Name("unlock")
public class UnlockCommand extends Command<StorageClient> {

    private String key;

    private int lockToken;

    public UnlockCommand(CLI cli) {
        super(cli);
    }

    @Argument(index = 0, argName = "key")
    public void setKey(String key) {
        this.key = key;
    }

    @Argument(index = 1, argName = "lock-token")
    public void setLockToken(int lockToken) {
        this.lockToken = lockToken;
    }

    @Override
    @SuppressWarnings("java:S106")
    public void execute(CommandLine commandLine, StorageClient client) {
        client.unlock(key, lockToken)
                .onSuccess(response -> System.out.println(getCli().getName() + " successful"))
                .onFailure(err -> System.err.println(err.getMessage()));
    }
}
