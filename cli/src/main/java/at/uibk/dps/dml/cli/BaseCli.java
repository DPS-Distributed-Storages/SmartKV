package at.uibk.dps.dml.cli;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.cli.CLIException;
import io.vertx.core.cli.CommandLine;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class BaseCli extends AbstractVerticle {

    @SuppressWarnings("java:S106")
    protected static <C> void start(C client, List<Class<? extends Command<C>>> commandClasses) {
        Map<String, Command<C>> commandMap = commandClasses
                .stream()
                .map(Command::createProxy)
                .collect(Collectors.toMap(cmd -> cmd.getCli().getName(), Function.identity()));

        while (true) {
            Scanner in = new Scanner(System.in);
            String line = in.nextLine();

            List<String> tokens = Arrays.asList(line.split(" "));
            if (tokens.isEmpty()) {
                continue;
            }

            String commandName = tokens.get(0);
            if (commandName.equals("exit")) {
                return;
            }

            Command<C> command = commandMap.get(commandName);
            if (command == null) {
                System.err.println("Unknown command");
                continue;
            }

            try {
                CommandLine commandLine = command.getCli().parse(tokens.subList(1, tokens.size()));

                if (!commandLine.isValid() && commandLine.isAskingForHelp()) {
                    // output help message
                    StringBuilder builder = new StringBuilder();
                    command.getCli().usage(builder);
                    System.out.println(builder);
                } else {
                    command.execute(commandLine, client);
                }
            } catch (CLIException e) {
                System.err.println("Invalid input: " + e.getMessage());
            }
        }
    }
}
