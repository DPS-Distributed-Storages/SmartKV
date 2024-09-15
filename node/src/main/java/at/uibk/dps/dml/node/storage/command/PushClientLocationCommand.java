package at.uibk.dps.dml.node.storage.command;

import at.uibk.dps.dml.node.util.NodeLocation;
import io.vertx.core.Promise;
import io.vertx.core.net.SocketAddress;

public class PushClientLocationCommand extends Command{

    protected NodeLocation clientLocation;

    protected SocketAddress clientSocketAddress;

    public PushClientLocationCommand() {
    }

    public PushClientLocationCommand(Promise<Object> promise, SocketAddress clientSocketAddress, NodeLocation clientLocation) {
        this(promise, clientSocketAddress, clientLocation.getRegion(), clientLocation.getZone(), clientLocation.getProvider());
    }

    public PushClientLocationCommand(Promise<Object> promise, SocketAddress clientSocketAddress, String clientRegion, String clientZone, String clientProvider) {
        super(promise, null, null, true, true);
        this.clientLocation = new NodeLocation(clientRegion, clientZone, clientProvider);
        this.clientSocketAddress = clientSocketAddress;
    }

    public String getClientRegion() {
        return clientLocation.getRegion();
    }

    public String getClientProvider() {
        return clientLocation.getProvider();
    }

    public NodeLocation getClientLocation() {
        return clientLocation;
    }

    public SocketAddress getClientSocketAddress() {
        return clientSocketAddress;
    }

    @Override
    public Object apply(CommandHandler handler) {
        return handler.apply(this);
    }


}
