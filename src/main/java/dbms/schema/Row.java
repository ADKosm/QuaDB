package dbms.schema;

import dbms.schema.dataTypes.Cell;

import java.util.List;
import java.util.stream.Collectors;

public class Row {
    private List<Cell> cells;

    public Row(List<Cell> c) {
        cells = c;
    }

    public List<Cell> getCells() {
        return cells;
    }

    public void setCells(List<Cell> cells) {
        this.cells = cells;
    }

    public short getRowSize() {
        short result = 0;
        for(Cell cell : cells) {
            result += cell.getByteSize();
        }
        result += 1; // byte for alive flag
        return result;
    }

    public String toString() {
        return cells.stream().map(Cell::toString).collect(Collectors.joining("|"));
    }

    public boolean equals(Row other) {
        boolean result = true;
        for(int i = 0; i < cells.size(); i++) {
            result = result && (cells.get(i).compareTo(other.getCells().get(i)) == 0);
        }
        return result; // TODO: add primary key and compare by it
    }
}
