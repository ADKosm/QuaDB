package dbms.index;

import dbms.Consts;
import dbms.query.Predicate;
import dbms.schema.Column;
import dbms.schema.Row;
import dbms.schema.dataTypes.Cell;
import dbms.schema.dataTypes.PagePointer;
import dbms.schema.dataTypes.Pointer;
import dbms.storage.BufferManager;
import dbms.storage.DataPage;
import dbms.storage.Page;
import dbms.storage.StorageManager;
import dbms.storage.table.RealTable;
import dbms.storage.table.Table;
import sun.awt.image.ImageWatched;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * Created by alex on 01.03.17.
 */

/**
 * |Index Link(Long-Pointer)|Value(ColumnSize-Cell)|DataLink(Long+Short-PagePointer)|...|Index Link(Long)|0|
 * Index Link != 0. => Index Link = pageIndex + 1, so pageIndex = IndexLink - 1
 */
public class BTree {
    private IndexSchema schema;
    private BufferManager<DataPage> bufferManager;
    private StorageManager storageManager = StorageManager.getInstance();
    private RealTable realTable;

    private Integer t;
    private BNode root;

    public BTree(IndexSchema indexSchema, RealTable table) {
        this.schema = indexSchema;
        bufferManager = new BufferManager<>(this.schema, DataPage::new);
        this.realTable = table;

        Integer recordSize = 0;
        for(Column column : schema.getColumns()) recordSize += column.getMaxSize() + 2; // 2 for shift table
        this.t = (Consts.BLOCK_SIZE - Long.BYTES - Long.BYTES - Byte.BYTES) / (2 * recordSize);

        root = readBNode(new Pointer(indexSchema.getRootPosition()));
        if(root == null) {
            root = allocateBNode();
            root.addLink(0, null).store();
        }
    }

    public void insert(Cell value, PagePointer pointer) {
        Triple<Cell, PagePointer, BNode> rightPart = insert(root, value, pointer);
        if(rightPart != null) {
            BNode newRoot = allocateBNode();
            newRoot.addData(0, rightPart.getLeft(), rightPart.getMiddle())
                    .addLink(0, root.getReference())
                    .addLink(1, rightPart.getRight().getReference())
                    .store();
            root = newRoot;
            schema.setRootPosition(root.getReference().getPointer());
            storageManager.updateIndexMeta(schema);
        }
    }

    public void search(Table result, Predicate predicate) {
        if(predicate.getOperator().equals("=")) {
            try{
                root.searchEqual(result, predicate.getColumn().createCell(predicate.getValue().toString()));
            } catch (Exception e) {}
        } else {
            root.searchRange(result, predicate);
        }
    }

    private Triple<Cell, PagePointer, BNode> insert(BNode node, Cell value, PagePointer pointer) {
        if(node == null) return Triple.build(value, pointer, null);

        ListIterator<Cell> position = node.findPlace(value);
        Triple<Cell, PagePointer, BNode> insertingData = insert(readBNode(node.linkAt(position.nextIndex())), value, pointer);
        if(insertingData == null) return null;

        node.addLink(position.nextIndex() + 1, insertingData.getRight().getReference())
                .addData(position.nextIndex(), insertingData.getLeft(), insertingData.getMiddle())
                .store();

        if(node.size() < 2*t - 1) return null;
        return node.cleave();
    }


    public class BNode {
        private LinkedList<Cell> data = new LinkedList<>(); // keys
        private List<Pointer> links = new ArrayList<>(); // links to BNodes
        private List<PagePointer> pointers = new ArrayList<>(); // pointers to real Data

        private Pointer reference;
        private Page page;

        public BNode(Pointer ref, Page page) {
            this.reference = ref;
            this.page = page;
        }

        public BNode(LinkedList<Cell> data,
                     List<Pointer> links,
                     List<PagePointer> pointers,
                     Pointer ref,
                     Page page) {
            this.data = data;
            this.links = links;
            this.pointers = pointers;
            this.reference = ref;
            this.page = page;
        }

        public Pointer linkAt(int index) {
            return links.get(index);
        }

        public Integer size() {
            return data.size();
        }

        public BNode addLink(int index, Pointer pointer) {
            links.add(index, pointer);
            return this;
        }

        public BNode addData(int index, Cell value, PagePointer pagePointer) {
            data.add(index, value);
            pointers.add(index, pagePointer);
            return this;
        }

        public Triple<Cell, PagePointer, BNode> cleave() {
            Cell mediana = data.get(t-1);
            PagePointer middlePointer = pointers.get(t-1);
            LinkedList<Cell> rightData = new LinkedList<>(data.subList(t, 2*t-1));
            List<PagePointer> rightPointers = new ArrayList<>(pointers.subList(t, 2*t-1));
            List<Pointer> rightLinks = new ArrayList<>(links.subList(t, 2*t));

            data.subList(t-1, 2*t-1).clear();
            pointers.subList(t-1, 2*t-1).clear();
            links.subList(t-1, 2*t).clear();

            store();

            BNode rightPart = allocateBNode(rightData, rightLinks, rightPointers);
            return Triple.build(mediana, middlePointer, rightPart);

        }

        public ListIterator<Cell> findPlace(Cell value) {
            ListIterator<Cell> iterator = data.listIterator();
            while (iterator.hasNext()) {
                if(value.compareTo(iterator.next()) < 0) {
                    iterator.previous();
                    break;
                }
            }
            return iterator;
        }


        public Pointer getReference() {
            return reference;
        }

        public BNode store() {
            ((DataPage)page).clear();
            for(int i = 0; i < data.size(); i++) {
                List<Cell> cells = new ArrayList<>();
                cells.add(links.get(i));
                cells.add(data.get(i));
                cells.add(pointers.get(i));
                Row row = new Row(cells);
                ((DataPage)page).insertValues(row); // TODO: restore single Page class
            }

            List<Cell> cells = new ArrayList<>();
            cells.add(links.get(links.size()-1));
            try{
                cells.add(schema.getIndexedColumn().createCell("0"));
            } catch (Exception e) {e.fillInStackTrace();}
            cells.add(new PagePointer((long)0, (short)0));
            Row row = new Row(cells);
            ((DataPage) page).insertValues(row);
            return this;
        }

        private void moveValue(Table table, ListIterator<Cell> iterator) {
            table.add(realTable.getRowAt(pointers.get(iterator.previousIndex())));
        }

        public void searchEqual(Table table, Cell value) {
            ListIterator<Cell> iterator = data.listIterator();
            while (iterator.hasNext()) {
                Cell currVal = iterator.next();
                if(value.compareTo(currVal) == 0) {
                    moveValue(table, iterator);
                    return;
                }
                if(value.compareTo(currVal) < 0) {
                    iterator.previous();
                    break;
                }
            }
            BNode next = readBNode(linkAt(iterator.nextIndex()));
            if(next != null) {
                next.searchEqual(table, value);
            }
        }

        public void searchRange(Table table, Predicate predicate) {
            ListIterator<Cell> iterator = data.listIterator();
            Integer c = predicate.getOperator().equals("<") ? 1 : -1;
            Cell value;
            try{
                value = predicate.getColumn().createCell(predicate.getValue().toString());
            }catch(Exception e) { return; }
            boolean lastEqual = false;
            while (iterator.hasNext()) {
                Cell currVal = iterator.next();
                if(value.compareTo(currVal) * c < 0) {
                    BNode next = readBNode(links.get(iterator.previousIndex()));
                    if(next != null) next.searchRange(table, predicate);
                    moveValue(table, iterator);
                    lastEqual = true;
                } else {
                    if(lastEqual) {
                        BNode next = readBNode(links.get(iterator.previousIndex()));
                        if(next != null) next.searchRange(table, predicate);
                    }
                    lastEqual = false;
                }
            }
            if(lastEqual) {
                BNode next = readBNode(links.get(iterator.nextIndex()));
                if(next != null) next.searchRange(table, predicate);
            }
        }
    }

    private BNode allocateBNode() {
        Page newPage = bufferManager.allocateNewPage();
        Pointer newPointer = new Pointer((long)(bufferManager.getPageCount()));
        return new BNode(newPointer, newPage);
    }

    private BNode allocateBNode(LinkedList<Cell> data, List<Pointer> links, List<PagePointer> pointers ) {
        Page newPage = bufferManager.allocateNewPage();
        Pointer newPointer = new Pointer((long)(bufferManager.getPageCount()));
        return new BNode(data, links, pointers, newPointer, newPage);
    }

    private BNode readBNode(Pointer pointer) {
        DataPage page = bufferManager.getPage((pointer.getPointer()-1));
        if(page == null) return null;
        LinkedList<Cell> data = new LinkedList<>();
        List<Pointer> links = new ArrayList<>();
        List<PagePointer> pointers = new ArrayList<>();

        List<Row> rows = page.toRows(schema);
        for(int i = 0; i < rows.size()-1; i++) {
            links.add((Pointer) rows.get(i).getCells().get(0));
            data.add(rows.get(i).getCells().get(1));
            pointers.add((PagePointer) rows.get(i).getCells().get(2));
        }
        links.add((Pointer) rows.get(rows.size()-1).getCells().get(0));
        return new BNode(data, links, pointers, pointer, page);
    } // Nullable

}

class Triple<L, M, R> {
    final L left;
    final M middle;
    final R right;

    public Triple(L left, M middle, R right) {
        this.left = left;
        this.middle = middle;
        this.right = right;
    }

    public L getLeft() {
        return left;
    }

    public R getRight() {
        return right;
    }

    public M getMiddle() {
        return middle;
    }

    public static <L, M, R> Triple<L, M, R> build(L left, M middle, R right) {
        return new Triple<L, M, R>(left, middle, right);
    }
}
