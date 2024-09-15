package at.uibk.dps.dml.cli.admin.storage.commands;

import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.cli.util.InputParserUtil;
import at.uibk.dps.dml.client.storage.StorageClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.*;

import java.util.List;

@Name("initObject")
public class InitObjectCommand extends Command<StorageClient> {

    private static final String DEFAULT_OBJECT_TYPE = "SharedBuffer";

    private String key;

    private String objectType;

    // We use a string list here to support spaces in the argument
    private List<String> jsonArgument;

    private Integer lockToken;

    public InitObjectCommand(CLI cli) {
        super(cli);
    }

    @Argument(index = 0, argName = "key")
    public void setKey(String key) {
        this.key = key;
    }

    @Option(argName = "object-type", longName = "object-type", shortName = "t")
    @Description("The object type (class name) of the object to be created. Defaults to " + DEFAULT_OBJECT_TYPE + ".")
    @DefaultValue(DEFAULT_OBJECT_TYPE)
    public void setObjectType(String objectType) {
        this.objectType = objectType;
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

    @Override
    @SuppressWarnings("java:S106")
    public void execute(CommandLine commandLine, StorageClient client) {
        Object[] args;
        try {
            args = InputParserUtil.jsonStringToObjectArray(String.join(" ", jsonArgument));
        } catch (JsonProcessingException e) {
            System.err.println("Could not parse JSON argument: " + e.getMessage());
            return;
        }

        client.initObject(key, objectType, args, lockToken)
                .onSuccess(response -> System.out.println(getCli().getName() + " successful"))
                .onFailure(err -> System.err.println(err.getMessage()));
    }
}
