package at.uibk.dps.dml.node.storage;
import java.util.HashMap;
import java.util.Map;

public class CustomClassLoader extends ClassLoader {

    private Map<String, Class<?>> loadedClasses;
    private final String sharedObjPrefixClientExtensions = "at.uibk.dps.dml.client.storage.object.extensions.";
    private final String sharedObjPrefixStorage = "at.uibk.dps.dml.node.storage.object.";

    public CustomClassLoader() {
        this.loadedClasses = new HashMap<>();
    }

    /**
     * Defines a class by its name and byte-code and appends its definition to a Map to cache it.
     * @param className - the class name
     * @param bytecode - the byte code of the class
     */
    public void defineAndCacheClass(String className, byte[] bytecode) {
        Class<?> clazz = loadedClasses.get(className);
        Class<?> clazz2 = loadedClasses.get(sharedObjPrefixStorage + className);
        if (clazz == null && clazz2 == null) {
            // If the class is not in the cache, load it using defineClass
            clazz = defineClass(sharedObjPrefixClientExtensions + className, bytecode, 0, bytecode.length);
            loadedClasses.put(sharedObjPrefixStorage + className, clazz);
        }
    }

    /**
     * Loads a Class using its fully qualified name. If the given Class name defines a user-defined shared object class and is cached in the loadedClasses map,
     * the class is directly searched and loaded by this {@link CustomClassLoader} instead of asking the parent ClassLoader first,
     * which would be the default behaviour in Java. This way we can enhance the performance of loading such extension classes.
     *
     * @param className – The binary name of the class
     *
     * @return The resulting Class object
     * @throws {@link ClassNotFoundException} – If the class was not found
     */
    @Override
    public Class loadClass(String className) throws ClassNotFoundException {
        // If the class was already defined and cached, directly load it with this Custom Classloader instead of asking the parent first to increase performance!
        if (loadedClasses.containsKey(className)) {
            return loadedClasses.get(className);
        }
        return super.loadClass(className);
    }

}
