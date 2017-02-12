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

    public String toString() {
        return cells.stream().map(Cell::toString).collect(Collectors.joining("|"));
    }
}
