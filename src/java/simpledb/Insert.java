package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private DbIterator iterator;
    private int tableId;
    private boolean calledOnce = false;

    /**
     * Constructor.
     * 
     * @author hrily
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        this.tid = t;
        this.iterator = child;
        this.tableId = tableid;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        iterator.open();
        super.open();
    }

    public void close() {
        super.close();
        iterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iterator.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(calledOnce) 
            return null;
        calledOnce = true;
        int nTuplesInserted = 0;
        BufferPool buffer = Database.getBufferPool();
        try {
            while(iterator.hasNext()){
                Tuple tuple = iterator.next();
                buffer.insertTuple(tid, tableId, tuple);
                nTuplesInserted++;
            }
        }catch(IOException ioe){
            throw new DbException("IOException: " + ioe.getMessage());
        }
        Tuple tuple = new Tuple(this.getTupleDesc());
        tuple.setField(0, new IntField(nTuplesInserted));
        return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{iterator};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        iterator = children[0];
    }
}
