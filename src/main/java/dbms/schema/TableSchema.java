package dbms.schema;

import dbms.schema.dataTypes.Cell;

import java.util.ArrayList;
import java.util.List;

public class TableSchema extends Schema{

    public TableSchema(List<Column> columns) {
        this.columns = columns;
    }

    public Column getColumn(String name) {
        return columns.stream().filter(x -> x.getName().equals(name)).findFirst().get();
    }

    public Row valuesToRow(List<String> values) throws Exception {
        if(values.size() != columns.size()) {
            throw new Exception("Incorrect shape of values"); // TODO: add NULL as value
        }
        List<Cell> cells = new ArrayList<Cell>();
        for(int i = 0; i < values.size(); i++) {
            cells.add(columns.get(i).createCell(values.get(i)));
        }
        return new Row(cells);
    }
}
