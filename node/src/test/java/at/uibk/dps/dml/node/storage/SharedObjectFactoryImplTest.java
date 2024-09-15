package at.uibk.dps.dml.node.storage;

import at.uibk.dps.dml.client.storage.BsonArgsCodec;
import at.uibk.dps.dml.node.storage.object.SharedClassDef;
import at.uibk.dps.dml.node.util.TestHelper;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.wildfly.common.Assert.assertTrue;

class SharedObjectFactoryImplTest {

    private SharedObjectFactory factory;

    @BeforeEach
    void beforeEach() {

        final String LUA_EXTENSION = SharedObjectFactoryImplTest.class.getClassLoader().getResource("Rectangle.lua").getPath();
        final String EXTENSION_CLASS = "Rectangle";
        final String EXTENSION_LANGUAGE = "lua";

        byte[] byteCode = TestHelper.readBytesFromFile(Path.of(LUA_EXTENSION));

        factory = new SharedObjectFactoryImpl(new BsonArgsCodec(), Vertx.vertx());

        factory.registerNewClass(new SharedClassDef(byteCode, EXTENSION_CLASS, EXTENSION_LANGUAGE));
    }

    @Test
    void testCreateJavaObject() {
        assertTrue(factory.createObject("java", "SharedBuffer", null) instanceof SharedJavaObjectWrapper);
    }

    @Test
    void testCreateLuaObject() throws IOException {

        BsonArgsCodec argsCodec = new BsonArgsCodec();
        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        Double length = 10.0;
        Double breadth = 5.0;
        byte[] argumsInit = argsCodec.encode(new Object[]{length,breadth});
        SharedObject sharedObject = factory.createObject("lua", "Rectangle", argumsInit);
        assertTrue(sharedObject instanceof SharedLuaObjectWrapper);
        SharedLuaObjectWrapper sharedLuaObjectWrapper = (SharedLuaObjectWrapper) sharedObject;

        Object resultObj = argsCodec.decode(sharedLuaObjectWrapper.invokeMethod("getArea", null))[0];
        Double area = mapper.readValue((String) resultObj, Double.class);
        assertTrue(area.equals(length * breadth));

        length = 2.0;
        breadth = 4.0;
        sharedLuaObjectWrapper.invokeMethod("init", argsCodec.encode(new Object[]{length,breadth}));

        Object resultObj2 = argsCodec.decode(sharedLuaObjectWrapper.get())[0];
        Rectangle rectangle = mapper.readValue((String) resultObj2, Rectangle.class);
        assertTrue(rectangle.getArea() == length * breadth);

        length = 20.0;
        breadth = 43.0;

        sharedLuaObjectWrapper.set(argsCodec.encode(new Object[]{length,breadth}));

        Object resultObj3 = argsCodec.decode(sharedLuaObjectWrapper.get())[0];
        Rectangle rectangle2 = mapper.readValue((String) resultObj3, Rectangle.class);
        assertTrue(rectangle2.getArea() == length * breadth);

    }

    @Test
    void testCreateObjectWithInvalidLanguageThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> factory.createObject("C+#thon", "SharedBuffer", null));
    }

}

class Rectangle{
    private double length;
    private double breadth;

    private double area;

    public Rectangle(){

    }

    public Rectangle(double length, double breadth) {
        this.length = length;
        this.breadth = breadth;
        this.area = breadth * length;
    }

    public Rectangle(double length, double breadth, double area) {
        this.length = length;
        this.breadth = breadth;
        this.area = area;
    }

    public double getLength() {
        return length;
    }

    public void setLength(double length) {
        this.length = length;
    }

    public double getBreadth() {
        return breadth;
    }

    public void setBreadth(double breadth) {
        this.breadth = breadth;
    }

    public double getArea() {
        return area;
    }

    public void setArea(double area) {
        this.area = area;
    }
}