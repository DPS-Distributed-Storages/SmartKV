package at.uibk.dps.dml.cli.commands;

import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.cli.util.InputParserUtil;
import at.uibk.dps.dml.client.DmlClient;
import at.uibk.dps.dml.client.storage.Flag;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.Argument;
import io.vertx.core.cli.annotations.Description;
import io.vertx.core.cli.annotations.Name;
import io.vertx.core.cli.annotations.Option;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

@Name("invokeMethod")
public class InvokeMethodCommand extends Command<DmlClient> {

    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private String key;

    private String method;

    // We use a string list here to support spaces in the argument
    private List<String> jsonArgument;

    private Integer lockToken;

    private boolean readOnly;

    public InvokeMethodCommand(CLI cli) {
        super(cli);
    }

    @Argument(index = 0, argName = "key")
    public void setKey(String key) {
        this.key = key;
    }

    @Argument(index = 1, argName = "method")
    public void setMethod(String method) {
        this.method = method;
    }

    @Option(argName = "argument", longName = "argument", shortName = "a")
    @Description("A string in JSON format containing the arguments for the method invocation.")
    public void setJsonArgument(List<String> jsonArgument) {
        this.jsonArgument = jsonArgument;
    }

    @Option(argName = "lock-token", longName = "lock-token", shortName = "l")
    public void setLockToken(Integer lockToken) {
        this.lockToken = lockToken;
    }

    @Option(argName = "read-only", longName = "read-only", shortName = "r", flag = true)
    @Description("Adds the read-only flag to the method invocation, thereby disabling replication. " +
            "Only valid if the method does not modify the object.")
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    @SuppressWarnings("java:S106")
    public void execute(CommandLine commandLine, DmlClient client) {
        Object[] args;
        try {
            args = InputParserUtil.jsonStringToObjectArray(String.join(" ", jsonArgument));
        } catch (JsonProcessingException e) {
            System.err.println("Could not parse JSON argument: " + e.getMessage());
            return;
        }

        Set<Flag> flags = EnumSet.noneOf(Flag.class);
        if (readOnly) {
            flags.add(Flag.READ_ONLY);
        }

        client.invokeMethod(key, method, args, lockToken, flags)
                .onSuccess(result -> {
                    try {
                        System.out.println(result == null ? "null" : jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
                    } catch (JsonProcessingException e) {
                        System.err.println("Could not parse result as JSON");
                    }
                })
                .onFailure(err -> System.err.println(err.getMessage()));
    }
}
