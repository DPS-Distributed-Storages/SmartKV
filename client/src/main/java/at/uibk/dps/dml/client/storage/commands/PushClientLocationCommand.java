package at.uibk.dps.dml.client.storage.commands;

import at.uibk.dps.dml.client.NodeLocation;
import at.uibk.dps.dml.client.Command;
import at.uibk.dps.dml.client.CommandType;
import at.uibk.dps.dml.client.storage.StorageCommandType;
import at.uibk.dps.dml.client.util.BufferUtil;
import at.uibk.dps.dml.client.util.ReadableBuffer;
import io.vertx.core.buffer.Buffer;

public class PushClientLocationCommand implements Command<Void> {

    private NodeLocation clientLocation;

    public PushClientLocationCommand(String clientRegion, String clientAccessPoint, String clientProvider) {
        this.clientLocation = new NodeLocation(clientRegion, clientAccessPoint, clientProvider);
    }

    public PushClientLocationCommand(NodeLocation clientLocation) {
        this.clientLocation = clientLocation;
    }


    @Override
    public CommandType getCommandType() {
        return StorageCommandType.PUSH_CLIENT_LOCATION;
    }

    @Override
    public void encode(Buffer buffer) {
        BufferUtil.appendLengthPrefixedString(buffer, clientLocation.getRegion());
        BufferUtil.appendLengthPrefixedString(buffer, clientLocation.getAccessPoint());
        BufferUtil.appendLengthPrefixedString(buffer, clientLocation.getProvider());
    }

    @Override
    public Void decodeReply(ReadableBuffer buffer) {
        return null;
    }
}