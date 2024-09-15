package at.uibk.dps.dml.node.storage;

import at.uibk.dps.dml.client.storage.SharedObjectArgsCodec;
import at.uibk.dps.dml.node.exception.SharedObjectCommandException;
import at.uibk.dps.dml.node.exception.SharedObjectException;
import org.luaj.vm2.*;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;
import org.luaj.vm2.lib.jse.CoerceLuaToJava;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;


/**
 * The {@link SharedLuaObjectWrapper} class wraps shared objects written in Lua and provides methods to invoke
 * their methods.
 */
public class SharedLuaObjectWrapper implements SharedObject {

   private final SharedObjectArgsCodec argsCodec;

    private final LuaFunction executorFunction;

    private LuaTable object;

    private final String objectType;

    private final LuaValue luaClass;


    /**
     * Creates a new shared object with the given type and arguments.
     *
     * @param argsCodec   the codec used to encode and decode the arguments
     * @param objectType  the type of the shared object
     * @param encodedArgs the encoded arguments to be passed to the constructor
     */
    public SharedLuaObjectWrapper(SharedObjectArgsCodec argsCodec, String objectType, byte[] encodedArgs, Globals globals, LuaFunction executorFunction) throws SharedObjectException {

        this.argsCodec = argsCodec;
        this.objectType = objectType;
        this.executorFunction = executorFunction;

        try {
            Object[] args = argsCodec.decode(encodedArgs);
            LuaValue[] luaArgs = Arrays.stream(args)
                    .map(CoerceJavaToLua::coerce)
                    .toArray(LuaValue[]::new);

            luaClass = globals.get(objectType);
            this.object = luaClass.invokemethod("new", luaArgs).arg(1).checktable();

        } catch (LuaError error) {
            throw new SharedObjectCommandException(
                    "Failed to create instance of lua type: " + objectType, error);
        }

    }

    /**
     * Invokes the method with the given name and the given arguments on the shared object.
     *
     * @param methodName  the name of the method to be invoked
     * @param encodedArgs the encoded arguments to be passed to the method
     * @return the encoded result of the method invocation
     */
    public byte[] invokeMethod(String methodName, byte[] encodedArgs) throws SharedObjectException {
        try {
            Object[] args = argsCodec.decode(encodedArgs);
            LuaValue[] luaValues = Arrays.stream(args)
                    .map(CoerceJavaToLua::coerce)
                    .toArray(LuaValue[]::new);

            LuaValue luaResult = executorFunction.invoke(LuaValue.varargsOf(LuaValue.valueOf(methodName), object, LuaValue.varargsOf(luaValues))).arg(1);
            String jsonResult = (String) CoerceLuaToJava.coerce(luaResult, String.class);

            return argsCodec.encode(new Object[]{jsonResult});
        } catch (LuaError error) {
            throw new SharedObjectCommandException(
                    "Failed to invoke method: " + methodName, error);
        }
    }

    /**
     * Sets the value of the shared object to the given arguments.
     * @param encodedArgs the encoded new value of the object
     */
    @Override
    public void set(byte[] encodedArgs) throws SharedObjectException {
        Object[] args = argsCodec.decode(encodedArgs);
        LuaValue[] luaArgs = Arrays.stream(args)
                .map(CoerceJavaToLua::coerce)
                .toArray(LuaValue[]::new);
        this.object = luaClass.invokemethod("new", luaArgs).arg(1).checktable();
    }

    /**
     * Returns the shared object.
     * @return the shared object
     */
    @Override
    public byte[] get() throws SharedObjectException {
        // We use the active KV here, as this is easier than parsing the LuaJ Table of this.object
        return invokeMethod("get", null);
    }

    @Override
    public String getLanguage() {
        return "lua";
    }

    @Override
    public Object getObject() {
        return object;
    }

    @Override
    public String getObjectType() {
        return objectType;
    }
}
