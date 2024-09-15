package at.uibk.dps.dml.cli.commands;

import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.cli.util.InputParserUtil;
import at.uibk.dps.dml.client.DmlClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.Argument;
import io.vertx.core.cli.annotations.Name;
import io.vertx.core.cli.annotations.Option;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Name("set")
public class SetCommand extends Command<DmlClient> {

    private String key;

    private List<String> value;

    private Integer lockToken;

    public SetCommand(CLI cli) {
        super(cli);
    }

    @Argument(index = 0, argName = "key")
    public void setKey(String key) {
        this.key = key;
    }

    @Argument(index = 1)
    public void setValue(List<String> value) {
        this.value = value;
    }

    @Option(argName = "lock-token", longName = "lock-token", shortName = "l")
    public void setLockToken(Integer lockToken) {
        this.lockToken = lockToken;
    }

    @Override
    @SuppressWarnings("java:S106")
    public void execute(CommandLine commandLine, DmlClient client) {
        Object[] args;
        try {
            args = InputParserUtil.jsonStringToObjectArray(String.join(" ", value));
        } catch (JsonProcessingException e) {
            System.err.println("Could not parse JSON argument: " + e.getMessage());
            return;
        }
        client.set(key, args, lockToken)
                .onSuccess(res -> System.out.println(getCli().getName() + " successful"))
                .onFailure(err -> System.err.println(err.getMessage()));
    }
}
