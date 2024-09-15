package at.uibk.dps.dml.node.statistics.command;

public interface CommandHandler {

    Object apply(GetStatisticsCommand command);

    Object apply(GetMainObjectLocationsCommand getMainObjectLocationsCommand);

    Object apply(GetUnitPricesCommand getUnitPricesCommand);

    Object apply(GetBillsCommand getBillsCommand);

    Object apply(ClearStatisticsCommand clearStatisticsCommand);

    Object apply(GetFreeMemoryCommand getFreeMemoryCommand);
}
