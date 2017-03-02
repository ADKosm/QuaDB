package dbms.storage;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.MappedByteBuffer;

/**
 * Created by alex on 01.03.17.
 */
public abstract class Page {
    public Page(MappedByteBuffer buffer) {
        throw new NotImplementedException();
    }
    public Page() {}
}
