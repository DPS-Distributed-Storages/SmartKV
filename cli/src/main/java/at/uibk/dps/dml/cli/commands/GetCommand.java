package at.uibk.dps.dml.cli.commands;

import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.client.DmlClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.Argument;
import io.vertx.core.cli.annotations.Name;
import io.vertx.core.cli.annotations.Option;

import java.nio.charset.StandardCharsets;

@Name("get")
public class GetCommand extends Command<DmlClient> {

    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private String key;

    private Integer lockToken;

    public GetCommand(CLI cli) {
        super(cli);
    }

    @Argument(index = 0, argName = "key")
    public void setKey(String key) {
        this.key = key;
    }

    @Option(argName = "lock-token", longName = "lock-token", shortName = "l")
    public void setLockToken(Integer lockToken) {
        this.lockToken = lockToken;
    }

    @Override
    @SuppressWarnings("java:S106")
    public void execute(CommandLine commandLine, DmlClient client) {
        client.get(key, lockToken)
                .onSuccess(
                        value -> {
                            try {
                                System.out.println(value == null ? "null" : jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(value));
                            } catch (JsonProcessingException e) {
                                System.err.println("Could not parse result as JSON");
                            }
                        })
                .onFailure(err -> System.err.println(err.getMessage()));
    }
}
