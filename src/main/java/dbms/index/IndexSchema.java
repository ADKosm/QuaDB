package dbms.index;

import dbms.Consts;
import dbms.schema.Column;
import dbms.schema.Schema;
import dbms.schema.dataTypes.PagePointer;

import java.util.List;

/**
 * Created by alex on 02.03.17.
 */
public class IndexSchema extends Schema {
    private Column column;
    private Long rootPosition;

    public IndexSchema(Column column) {
        this.column = column;
        columns.add(new Column("pointer", Consts.COLUMN_TYPE_POINTER));
        columns.add(column);
        columns.add(new Column("link", Consts.COLUMN_TYPE_PAGEPOINTER));
    }

    public Column getIndexedColumn() {
        return column;
    }

    public Long getRootPosition() {
        return rootPosition;
    }

    public void setRootPosition(Long rootPosition) {
        this.rootPosition = rootPosition;
    }

    public String serialize() {
        return rootPosition.toString();
    }
}
