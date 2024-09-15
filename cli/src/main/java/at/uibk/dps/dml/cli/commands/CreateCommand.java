package at.uibk.dps.dml.cli.commands;

import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.cli.util.InputParserUtil;
import at.uibk.dps.dml.client.DmlClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.*;

import java.util.LinkedHashSet;
import java.util.List;

@Name("create")
public class CreateCommand extends Command<DmlClient> {

    private static final String DEFAULT_OBJECT_TYPE = "SharedBuffer";

    private static final String DEFAULT_OBJECT_LANGUAGE = "java";

    private String key;

    private List<Integer> replicas;

    private String language;

    private String objectType;

    // We use a string list here to support spaces in the argument
    private List<String> jsonArgument;

    public CreateCommand(CLI cli) {
        super(cli);
    }

    @Argument(index = 0, argName = "key")
    public void setKey(String key) {
        this.key = key;
    }

    @Option(argName = "replicas", longName = "replicas", shortName = "r")
    @Description("The verticle IDs storing replicas. Multiple verticle IDs need to be separated by a comma.")
    @ParsedAsList
    public void setReplicas(List<Integer> replicas) {
        this.replicas = replicas;
    }

    @Option(argName = "language", longName = "language", shortName = "l")
    @Description("The object's language (programming language) it was written in. Defaults to " + DEFAULT_OBJECT_LANGUAGE + ".")
    @DefaultValue(DEFAULT_OBJECT_LANGUAGE)
    public void setLanguage(String language) {
        this.language = language;
    }

    @Option(argName = "object-type", longName = "object-type", shortName = "t")
    @Description("The object type (class name) of the object to be created. Defaults to " + DEFAULT_OBJECT_TYPE + ".")
    @DefaultValue(DEFAULT_OBJECT_TYPE)
    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }

    @Option(argName = "argument", longName = "argument", shortName = "a")
    @Description("A string in JSON format containing the arguments for the object to be created.")
    public void setJsonArgument(List<String> jsonArgument) {
        this.jsonArgument = jsonArgument;
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

        client.create(key, !replicas.isEmpty() ? new LinkedHashSet<>(replicas) : null, false, language, objectType, args, false)
                .onSuccess(res -> System.out.println(getCli().getName() + " successful"))
                .onFailure(err -> System.err.println(err.getMessage()));
    }
}
