package at.uibk.dps.dml.client.storage;

import at.uibk.dps.dml.client.BaseTcpClient;
import at.uibk.dps.dml.client.NodeLocation;
import at.uibk.dps.dml.client.storage.commands.*;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.Set;

public class StorageClient extends BaseTcpClient {

    protected final SharedObjectArgsCodec soArgsCodec;

    public StorageClient(Vertx vertx) {
        this(vertx, new BsonArgsCodec());
    }

    public StorageClient(Vertx vertx, SharedObjectArgsCodec soArgsCodec) {
        super(vertx);
        this.soArgsCodec = soArgsCodec;
    }

    public Future<Response<Integer>> lock(String key) {
        return request(new LockCommand(key));
    }

    public Future<Response<Void>> unlock(String key, int lockToken) {
        return request(new UnlockCommand(key, lockToken));
    }

    public Future<Response<Void>> initObject(String key, String objectType, Object[] args, Integer lockToken) {
        return initObject(key, "java", objectType, args, lockToken);
    }

    public Future<Response<Void>> initObject(String key, String languageId, String objectType, Object[] args, Integer lockToken) {
        return request(new InitObjectCommand(soArgsCodec, key, languageId, objectType, args, lockToken));
    }

    public Future<Response<Void>> set(String key, Object[] args, Integer lockToken, Set<Flag> flags) {
        return request(new SetCommand(soArgsCodec, key, args, lockToken, flags));
    }

    public Future<Response<Object>> get(String key, Integer lockToken, Set<Flag> flags) {
        return request(new GetCommand(soArgsCodec, key, lockToken, flags));
    }

    public Future<Response<Object>> invokeMethod(String key, String methodName, Object[] args, Integer lockToken, Set<Flag> flags) {
        return request(new InvokeMethodCommand(soArgsCodec, key, methodName, args, lockToken, flags));
    }

    public Future<Void> pushClientLocation(NodeLocation clientLocation){
        return request(new PushClientLocationCommand(clientLocation));
    }

    @Override
    protected StorageCommandError decodeCommandResultError(int errorCode, String message) {
        return new StorageCommandError(StorageCommandErrorType.valueOf(errorCode), message);
    }
}
