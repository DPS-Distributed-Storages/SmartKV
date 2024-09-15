package at.uibk.dps.dml.node.storage;

import at.uibk.dps.dml.client.storage.SharedObjectArgsCodec;
import at.uibk.dps.dml.node.exception.SharedObjectCommandException;
import at.uibk.dps.dml.node.exception.SharedObjectException;
import at.uibk.dps.dml.node.exception.SharedObjectReflectionException;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * The {@link SharedJavaObjectWrapper} class wraps shared objects written in Java and provides methods to invoke
 * their methods using reflection.
 */
public class SharedJavaObjectWrapper implements SharedObject {

    private static final String SHARED_OBJECTS_PACKAGE_PREFIX = "at.uibk.dps.dml.node.storage.object.";

    private final SharedObjectArgsCodec argsCodec;

    private Serializable object;

    /**
     * Creates a new shared object with the given type and arguments.
     *
     * @param argsCodec   the codec used to encode and decode the arguments
     * @param objectType  the type of the shared object
     * @param encodedArgs the encoded arguments to be passed to the constructor
     * @param classLoader the custom class loader
     */
    public SharedJavaObjectWrapper(SharedObjectArgsCodec argsCodec, String objectType, byte[] encodedArgs, CustomClassLoader classLoader) throws SharedObjectException {
        this.argsCodec = argsCodec;
        try {
            Object[] args = argsCodec.decode(encodedArgs);
            Class<?> clazz = classLoader.loadClass(SHARED_OBJECTS_PACKAGE_PREFIX + objectType);
            this.object = (Serializable) ConstructorUtils.invokeConstructor(clazz, args);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException e) {
            throw new SharedObjectReflectionException("Failed to invoke constructor of class: " + objectType, e);
        } catch (InvocationTargetException e) {
            throw new SharedObjectCommandException(
                    "Failed to invoke constructor of class: " + objectType, e.getCause());
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
            Object result = MethodUtils.invokeMethod(object, methodName, args);
            return argsCodec.encode(new Object[]{result});
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new SharedObjectReflectionException("Failed to invoke method: " + methodName, e);
        } catch (InvocationTargetException e) {
            throw new SharedObjectCommandException("Failed to invoke method: " + methodName, e.getCause());
        }
    }

    /**
     * Sets the value of the shared object to the given arguments.
     * @param encodedArgs the encoded new value of the object
     */
    @Override
    public void set(byte[] encodedArgs) throws SharedObjectException {
        try {
            Object[] args = argsCodec.decode(encodedArgs);
            this.object = ConstructorUtils.invokeConstructor(object.getClass(), args);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException e) {
            throw new SharedObjectReflectionException("Failed to invoke constructor of class: " + object.getClass().getSimpleName(), e);
        } catch (InvocationTargetException e) {
            throw new SharedObjectCommandException("Failed to set value", e.getCause());
        }
    }

    @Override
    public byte[] get() throws SharedObjectException {
        return argsCodec.encode(new Object[]{object});
    }

    @Override
    public String getLanguage() {
        return "java";
    }

    @Override
    public Serializable getObject() {
        return object;
    }

    @Override
    public String getObjectType() {
        return object.getClass().getSimpleName();
    }
}
