package at.uibk.dps.dml.cli;

import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;
import io.vertx.core.cli.annotations.CLIConfigurator;

public abstract class Command<C> {

    private final CLI cli;

    protected Command(CLI cli) {
        this.cli = cli;
    }

    public CLI getCli() {
        return cli;
    }

    static <C> Command<C> createProxy(Class<? extends Command<C>> commandClass) {
        CLI cli = CLI.create(commandClass);

        // add help option to the interface
        cli.addOption(new Option().setArgName("help").setLongName("help").setShortName("h").setFlag(true).setHelp(true));

        return new Command<>(cli) {
            @Override
            public void execute(CommandLine commandLine, C client) {
                // create a new instance of the command class; inject the values from the parsed input; execute
                Command<C> instance;
                try {
                    instance = commandClass.getDeclaredConstructor(CLI.class).newInstance(cli);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                CLIConfigurator.inject(commandLine, instance);
                instance.execute(commandLine, client);
            }
        };
    }

    public abstract void execute(CommandLine commandLine, C client);
}
