package at.uibk.dps.dml.node.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class TestHelper {

    public static byte[] readBytesFromClass(Class clazz) {
        String path = clazz.getName().replace('.', File.separatorChar) + ".class";
        InputStream is = clazz.getClassLoader().getResourceAsStream(path);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int data = -1;
        while (true) {
            try {
                if (!((data = is.read()) != -1)) break;
            } catch (IOException e) {
                System.err.println("Could not read the class file: " + e.getMessage());
            }
            bos.write(data);

        }
        return bos.toByteArray();
    }

    public static byte[] readBytesFromFile(Path path){

        byte[] byteCode = null;

        try {
            byteCode = Files.readAllBytes(path);
        } catch (IOException e) {
            System.err.println("Could not read the class file: " + e.getMessage());
            e.printStackTrace();
        }

        return byteCode;

    }

}
