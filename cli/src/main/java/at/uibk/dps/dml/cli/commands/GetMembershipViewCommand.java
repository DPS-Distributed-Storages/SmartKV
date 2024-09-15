package at.uibk.dps.dml.cli.commands;

import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.client.DmlClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.Name;

@Name("membership")
public class GetMembershipViewCommand extends Command<DmlClient> {

    private static final ObjectMapper jsonMapper = new ObjectMapper();

    public GetMembershipViewCommand(CLI cli) {
        super(cli);
    }

    @Override
    @SuppressWarnings("java:S106")
    public void execute(CommandLine commandLine, DmlClient client) {
        client.getMembershipView()
                .onSuccess(membershipViewJson -> {
                    try {
                        Object membershipView = jsonMapper.readValue(membershipViewJson, Object.class);
                        System.out.println(membershipViewJson == null ? "null" : jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(membershipView));
                    } catch (JsonProcessingException e) {
                        System.err.println("Could not parse result as JSON");
                    }
                })
                .onFailure(err -> System.err.println(err.getMessage()));
    }
}
