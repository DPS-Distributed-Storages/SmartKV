package at.uibk.dps.dml.node.storage;

import at.uibk.dps.dml.client.storage.SharedObjectArgsCodec;
import at.uibk.dps.dml.node.exception.SharedObjectCommandException;
import at.uibk.dps.dml.node.storage.object.SharedClassDef;
import at.uibk.dps.dml.node.exception.SharedObjectException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.*;
import org.luaj.vm2.*;
import org.luaj.vm2.compiler.LuaC;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * The default {@link SharedObjectFactory} implementation.
 */
public class SharedObjectFactoryImpl implements SharedObjectFactory {

    protected final SharedObjectArgsCodec argsCodec;

    // for Java
    private final CustomClassLoader classLoader;

    // for Lua
    private final Globals globals;

    // for Lua
    private final LuaFunction executorFunction;

    /**
     * Default constructor.
     *
     * @param argsCodec the codec used to encode/decode arguments
     */
    public SharedObjectFactoryImpl(SharedObjectArgsCodec argsCodec, Vertx vertx) {
        this.argsCodec = argsCodec;
        classLoader = new CustomClassLoader();
        globals = JsePlatform.standardGlobals();

        // Load the Lua JSON lib once
        loadJsonLib();

        // Load the Lua Executor lib once
        this.executorFunction = loadExecutorFunction();
    }

    @Override
    public SharedObject createObject(String languageId, String objectType, byte[] encodedArgs) throws SharedObjectException {
        if (languageId.equals("java")) {
            SharedJavaObjectWrapper sharedJavaObjectWrapper = new SharedJavaObjectWrapper(argsCodec, objectType, encodedArgs, classLoader);
            // If we pushed a new object of type SharedClassDef, we have to register this class extension
            if (objectType.equals(at.uibk.dps.dml.node.storage.object.SharedClassDef.class.getSimpleName())) {
                registerNewClass((SharedClassDef) sharedJavaObjectWrapper.getObject());
            }
            return sharedJavaObjectWrapper;
        } else if (languageId.equals("lua")) {
            SharedLuaObjectWrapper sharedLuaObjectWrapper = new SharedLuaObjectWrapper(argsCodec, objectType, encodedArgs, globals, executorFunction);
            return sharedLuaObjectWrapper;
        }
        throw new IllegalArgumentException("Language not supported: " + languageId);
    }

    public CustomClassLoader getClassLoader() {
        return this.classLoader;
    }

    @Override
    public void registerNewClass(SharedClassDef clazz) {
        // If it is a java class, we register it at our CustomClassLoader to be able to use it later for creating objects!
        switch (clazz.getLanguage()) {
            case "java":
                try {
                    classLoader.defineAndCacheClass(clazz.getClassName(), clazz.getByteCode());
                } catch (ClassFormatError e) {
                    throw new SharedObjectCommandException("The provided java Class " + clazz.getClassName() + " could not be registered - the byte code does not define a valid class!", e);
                } catch (NoClassDefFoundError e) {
                    throw new SharedObjectCommandException("The provided java Class " + clazz.getClassName() + " could not be registered - the name was wrong! Check the correct package and name usage!", e);
                }
                break;
            case "lua":
                registerLuaScript(clazz);
                break;
            default:
                throw new IllegalArgumentException("Extension language not supported: " + clazz.getLanguage());
        }
    }

    private void registerLuaScript(SharedClassDef clazz) {
        try {
            LuaValue chunk = globals.load(new String(clazz.getByteCode(), StandardCharsets.UTF_8), clazz.getClassName() + ".lua");
            chunk.call();
        } catch (LuaError e) {
            throw new SharedObjectCommandException("The provided lua script " + clazz.getClassName() + " could not be registered! The script is malformed and could not be compiled! ", e);
        }
    }

    private void loadJsonLib() {
        InputStream jsonStream = classLoader.getResourceAsStream("storage_extensions/JSON.lua");
        try {
            // Read the Lua Json script content
            byte[] scriptBytes = jsonStream.readAllBytes();
            String luaScript = new String(scriptBytes, StandardCharsets.UTF_8);
            // Load Lua JSON script into LuaJ
            LuaValue jsonLib = globals.load(luaScript, "JSON.lua");
            globals.set("JSON", jsonLib.call());
        } catch (IOException | LuaError e) {
            throw new SharedObjectCommandException("The provided lua JSON script could not be registered!", e);
        } finally {
            try {
                jsonStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private LuaFunction loadExecutorFunction(){
        InputStream executorStream = classLoader.getResourceAsStream("storage_extensions/Executor.lua");
        try {
            // Read the Lua script content
            byte[] scriptBytes = executorStream.readAllBytes();
            String luaScript = new String(scriptBytes, StandardCharsets.UTF_8);

            LuaValue executorLib = globals.load(luaScript, "Executor.lua");
            LuaTable executor = executorLib.invoke().arg(1).checktable();
            return executor.get("invokeMethod").checkfunction();
        } catch (IOException | LuaError e) {
            throw new SharedObjectCommandException("The provided lua Executor script could not be registered!", e);
        } finally {
            try {
                executorStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
