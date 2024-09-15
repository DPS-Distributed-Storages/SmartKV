package at.uibk.dps.dml.cli.commands;
import at.uibk.dps.dml.cli.Command;
import at.uibk.dps.dml.client.DmlClient;
import at.uibk.dps.dml.client.storage.object.*;
import at.uibk.dps.dml.client.storage.object.SharedClassDef;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.annotations.*;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;

@Name("register")
public class RegisterClassCommand extends Command<DmlClient> {

    private final String EXTENSION_PACKAGE = "at.uibk.dps.dml.client.storage.object.extensions.";
    private final Path EXTENSION_PACKAGE_PATH = Path.of("./client/src/main/java/at/uibk/dps/dml/client/storage/object/extensions/");

    private Path path = null;

    private String className;

    private String language;

    private List<Integer> replicas;

    private String objectType = SharedClassDef.class.getTypeName();

    public RegisterClassCommand(CLI cli) {
        super(cli);
    }

    @Argument(index = 0, argName = "language")
    @Description("The language (e.g. java or lua) in which the class that should be registered is implemented.")
    public void setLanguage(String language) {
        this.language = language;
    }

    @Argument(index = 1, argName = "className")
    @Description("The simple name of the class that should be registered (E.g. UserDefinedClass).")
    public void setClassName(String className) {
        this.className = className;
    }

    @Argument(index = 2, argName = "path")
    @Description("The absolute path/location of the folder where the file of this extension class that shall be loaded is stored.")
    public void setPath(String path) {
        this.path = Path.of(path);
    }

    @Option(argName = "replicas", longName = "replicas", shortName = "r")
    @Description("The verticle IDs storing replicas. Multiple verticle IDs need to be separated by a comma.")
    @ParsedAsList
    public void setReplicas(List<Integer> replicas) {
        this.replicas = replicas;
    }

    @Override
    @SuppressWarnings("java:S106")
    public void execute(CommandLine commandLine, DmlClient client) {
        switch(language){
            case "java":
                registerJava(client);
                break;
            case "lua":
                registerLua(client);
                break;
            default:
                System.err.printf("Language %s not supported!\n", language);
        }
    }

    private void registerJava(DmlClient client){

        byte[] byteCode = readBytesFromFile(path);

        if(byteCode == null){
            return;
        }

        client.registerSharedJavaClass(className, byteCode).onSuccess(res -> System.out.println(getCli().getName() + " successful"))
                .onFailure(err -> System.err.println(err.getMessage()));
    }

    private void registerLua(DmlClient client){

        byte[] byteCode = readBytesFromFile(path);

        if(byteCode == null){
            return;
        }

        client.registerSharedLuaClass(className, byteCode).onSuccess(res -> System.out.println(getCli().getName() + " successful"))
                .onFailure(err -> System.err.println(err.getMessage()));
    }

    private byte[] readBytesFromFile(Path path){

        byte[] byteCode = null;

        try {
            byteCode = Files.readAllBytes(path);
        } catch (IOException e) {
            System.err.println("Could not read the class file: " + e.getMessage());
            e.printStackTrace();
        }

        return byteCode;

    }

    private byte[] readBytesFromClass(Class clazz){
        Path path = Path.of(clazz.getName().replace('.', File.separatorChar) + ".class");
        InputStream is = clazz.getClassLoader().getResourceAsStream(path.toString());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int data = -1;
        while(true) {
            try {
                if (!((data=is.read())!=-1)) break;
            } catch (IOException e) {
                System.err.println("Could not read the class file: " + e.getMessage());
            }
            bos.write(data);

        }
        return bos.toByteArray();
    }

}
