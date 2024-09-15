package at.uibk.dps.dml.node.storage.object;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Predicate;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;

/**
 * The {@link SharedJson} holds a shared JSON document.
 */
public class SharedJson implements Serializable {

    protected transient DocumentContext json;

    /**
     * Default constructor to initialize an empty JSON document.
     */
    public SharedJson() {
        this.json = initContext();
    }

    /**
     * Constructor to initialize a JSON document with an initial value.
     *
     * @param json the initial JSON value
     */
    public SharedJson(Object json) {
        this.json = JsonPath.parse(json);
    }

    /**
     * Sets the JSON document to the given value.
     *
     * @param value the value to be set
     */
    public void set(Object value) {
        json = JsonPath.parse(value);
    }

    /**
     * Sets the value at the given path.
     *
     * @param path the JSONPath to the value to be set
     * @param value the value to be set
     */
    public void set(String path, Object value) {
        if (path.equals("$")) {
            // We cannot set the root (see https://github.com/json-path/JsonPath/issues/880),
            // so we overwrite the entire document as a workaround
            set(value);
            return;
        }
        this.json.set(path, value);
    }

    /**
     * Returns the value at the given path.
     *
     * @param path the JSONPath to the value to be returned
     * @return the value at the given path
     */
    public Object get(String path) {
        return json.read(path);
    }

    /**
     * Adds or updates the key with the given value at the given path.
     *
     * @param path the JSONPath to the key
     * @param key the key to add or update
     * @param value the value to add or update
     */
    public void put(String path, String key, Object value) {
        this.json.put(path, key, value);
    }

    /**
     * Adds the given value to the array at the given path.
     *
     * @param path the JSONPath to the array
     * @param value the value to add
     * @see DocumentContext#add(String, Object, Predicate...)
     */
    public void add(String path, Object value) {
        this.json.add(path, value);
    }

    /**
     * Deletes the value at the given path.
     *
     * @param path the JSONPath to the value to be deleted
     */
    public void delete(String path) {
        if (path.equals("$")) {
            this.json = initContext();
            return;
        }
        this.json.delete(path);
    }

    /**
     * Returns a string representation of the JSON document.
     *
     * @return a string representation of the JSON document
     */
    @JsonIgnore
    public String getAsString() {
        return json.jsonString();
    }

    /**
     * Returns the JSON document.
     *
     * @return the JSON document
     */
    @JsonProperty("json")
    public Object getJson() {
        return json.json();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SharedJson json1 = (SharedJson) o;
        return json.json().equals(json1.json.json());
    }

    @Override
    public int hashCode() {
        return Objects.hash(json.json());
    }

    private DocumentContext initContext() {
        return JsonPath.parse("{}");
    }

    private void readObject(ObjectInputStream inputStream) throws ClassNotFoundException, IOException {
        inputStream.defaultReadObject();
        // read transient fields
        json = JsonPath.parse(inputStream.readObject());
    }

    private void writeObject(ObjectOutputStream outputStream) throws IOException {
        outputStream.defaultWriteObject();
        // write transient fields
        outputStream.writeObject(json.json());
    }
}
