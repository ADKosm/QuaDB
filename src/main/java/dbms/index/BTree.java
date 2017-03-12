package dbms.index;

import dbms.Consts;
import dbms.query.Predicate;
import dbms.schema.Column;
import dbms.schema.Row;
import dbms.schema.dataTypes.Cell;
import dbms.schema.dataTypes.PagePointer;
import dbms.schema.dataTypes.Pointer;
import dbms.storage.BufferManager;
import dbms.storage.BufferedStorage;
import dbms.storage.Page;
import dbms.storage.StorageManager;
import dbms.storage.table.RealTable;
import dbms.storage.table.Table;

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

// TODO: !!! DO REFACTORING! Get rid of workarounds

/**
 * Too slow inserting - with transaction, it can be faster
 * Predicate must be more flexible - by now, only "column <(or>) number" format
 * Bufferize BNodes
 */
public class BTree {
    // TODO: dave loaded page in BufferedStorage
    private BufferedStorage<Pointer, BNode> bufferedStorage = new BufferedStorage<>();
    private IndexSchema schema;
    private BufferManager bufferManager;
    private StorageManager storageManager = StorageManager.getInstance();
    private RealTable realTable;

    private Integer t;
    private BNode root;

    public BTree(IndexSchema indexSchema, RealTable table) {
        this.schema = indexSchema;
        bufferManager = new BufferManager(schema);
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

        Pointer nextRef = insertingData.getRight() == null ? null : insertingData.getRight().getReference();

        node.addLink(position.nextIndex() + 1, nextRef)
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

            BNode rightPart = allocateBNode(rightData, rightLinks, rightPointers).store();
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
            (page).clear();
            for(int i = 0; i < data.size(); i++) {
                List<Cell> cells = new ArrayList<>();
                if(links.get(i) != null) {
                    cells.add(links.get(i));
                } else {
                    cells.add(new Pointer((long)0));
                }
                cells.add(data.get(i));
                cells.add(pointers.get(i));
                Row row = new Row(cells);
                (page).insertValues(row); // TODO: restore single Page class
            }

            List<Cell> cells = new ArrayList<>();
            if(links.get(links.size()-1) != null) {
                cells.add(links.get(links.size()-1));
            } else {
                cells.add(new Pointer((long)0));
            }
            try{
                cells.add(schema.getIndexedColumn().createCell("0"));
            } catch (Exception e) {e.fillInStackTrace();}
            cells.add(new PagePointer((long)0, (short)0));
            Row row = new Row(cells);
            ( page).insertValues(row);
            return this;
        }

        private void moveValue(Table table, Integer index) {
            table.add(realTable.getRowAt(pointers.get(index)));
        }

        public void searchEqual(Table table, Cell value) {
            ListIterator<Cell> iterator = data.listIterator();
            while (iterator.hasNext()) {
                Cell currVal = iterator.next();
                if(value.compareTo(currVal) == 0) {
                    moveValue(table, iterator.previousIndex());
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

        public void searchRange(Table table, Predicate predicate) { // TODO: refactor PLES!!!
            if(data.size() == 0) return;

            Cell value;
            try{
                value = predicate.getColumn().createCell(predicate.getValue().toString());
            }catch(Exception e) { return; }

            if(predicate.getOperator().equals("<")) {
                ListIterator<Cell> iterator = data.listIterator();
                while(iterator.hasNext()) {
                    Cell currVal = iterator.next();
                    BNode next = readBNode(links.get(iterator.previousIndex()));
                    if(next != null) next.searchRange(table, predicate);
                    if(currVal.compareTo(value) < 0) {
                        moveValue(table, iterator.previousIndex());
                    } else {
                        break;
                    }
                }
                if(iterator.previous().compareTo(value) < 0) {
                    BNode next = readBNode(links.get(iterator.nextIndex()));
                    if(next != null) next.searchRange(table, predicate);
                }
            } else {
                ListIterator<Cell> iterator = data.listIterator(data.size());
                while(iterator.hasPrevious()) {
                    Cell currVal = iterator.previous();
                    BNode next = readBNode(links.get(iterator.nextIndex()+1));
                    if(next != null) next.searchRange(table, predicate);
                    if(currVal.compareTo(value) > 0) {
                        moveValue(table, iterator.nextIndex());
                    } else {
                        break;
                    }
                }
                if(iterator.next().compareTo(value) > 0) {
                    BNode next = readBNode(links.get(iterator.previousIndex()));
                    if(next != null) next.searchRange(table, predicate);
                }
            }
        }
    }

    private BNode allocateBNode() {
        Page newPage = bufferManager.allocateNewPage();
        Pointer newPointer = new Pointer((long)(bufferManager.getPageCount()));

        BNode newNode = new BNode(newPointer, newPage);
        bufferedStorage.add(newPointer, newNode);
        return newNode;
    }

    private BNode allocateBNode(LinkedList<Cell> data, List<Pointer> links, List<PagePointer> pointers ) {
        Page newPage = bufferManager.allocateNewPage();
        Pointer newPointer = new Pointer((long)(bufferManager.getPageCount()));

        BNode newNode = new BNode(data, links, pointers, newPointer, newPage);
        bufferedStorage.add(newPointer, newNode);
        return newNode;
    }

    private BNode readBNode(Pointer pointer) {
        if(pointer == null ) return null;

        System.out.println("Quering bnode: " + pointer.getPointer().toString());
        if(bufferedStorage.contains(pointer)) {
            System.out.println("Cached bnode: " + pointer.getPointer().toString());
            return bufferedStorage.get(pointer);
        }

        Page page = bufferManager.getPage((pointer.getPointer()-1));
        if(page == null) return null;
        LinkedList<Cell> data = new LinkedList<>();
        List<Pointer> links = new ArrayList<>();
        List<PagePointer> pointers = new ArrayList<>();

        if(page.getRecordCount() > 0) {
            List<Row> rows = page.toRows(schema);
            for(int i = 0; i < rows.size()-1; i++) {
                Pointer p = (Pointer) rows.get(i).getCells().get(0);
                links.add(p.getPointer() == 0 ? null : p);
                data.add(rows.get(i).getCells().get(1));
                pointers.add((PagePointer) rows.get(i).getCells().get(2));
            }
            Pointer p = (Pointer) rows.get(rows.size()-1).getCells().get(0);
            links.add(p.getPointer() == 0 ? null : p);
        }

        BNode node = new BNode(data, links, pointers, pointer, page);
        bufferedStorage.add(pointer, node);
        return node;
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
