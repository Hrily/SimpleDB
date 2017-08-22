package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    
    TransactionId tid;
    int tableId;
    String tableAlias;
    
    DbFileIterator iterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @author hrily
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @author hrily
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }
    
    /**
     * @author hrily
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        if(tableAlias == null)
            return "null";
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * 
     * @author hrily
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    /**
     * Opens the Scanner for reading
     * 
     * @author hrily
     * @throws DbException
     * @throws TransactionAbortedException 
     */
    public void open() throws DbException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDbFile(tableId);
        iterator = file.iterator(tid);
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @author hrily
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return Database.getCatalog().getTupleDesc(tableId);
    }

    /**
     * Returns true if the iterator still has tuples
     * 
     * @author hrily
     * @return boolean
     * @throws TransactionAbortedException
     * @throws DbException 
     */
    public boolean hasNext() throws TransactionAbortedException, DbException {
        return iterator.hasNext();
                
    }

    /**
     * Return next tuple in the iterator
     * 
     * @return Tuple
     * @throws NoSuchElementException
     * @throws TransactionAbortedException
     * @throws DbException 
     */
    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return iterator.next();
    }

    /**
     * Closes the iterator
     * 
     * @author hrily
     */
    public void close() {
        iterator.close();
    }

    /**
     * Rewinds the iterator to the start
     * 
     * @author hrily
     * @throws DbException
     * @throws NoSuchElementException
     * @throws TransactionAbortedException 
     */
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        iterator.rewind();
    }
}
