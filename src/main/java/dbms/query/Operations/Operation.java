package dbms.query.Operations;

import dbms.query.Computator;
import dbms.schema.SchemaManager;
import dbms.storage.BufferManager;
import dbms.storage.DiskManager;

/**
 * Created by alex on 12.02.17.
 */
public interface Operation {
    void compute(Computator computator);
}
