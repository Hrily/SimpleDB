package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private DbIterator iterator;
    private boolean calledOnce = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @author hrily
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.iterator = child;
    }

    public TupleDesc getTupleDesc() {
        return iterator.getTupleDesc();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        iterator.open();
        super.open();
    }

    @Override
    public void close() {
        super.close();
        iterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iterator.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @throws simpledb.TransactionAbortedException
     * @throws simpledb.DbException
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(calledOnce)
            return null;
        calledOnce = true;
        int nTuplesDeleted = 0;
        BufferPool buffer = Database.getBufferPool();
        while(iterator.hasNext()){
            Tuple tuple = iterator.next();
            buffer.deleteTuple(tid, tuple);
            nTuplesDeleted++;
        }
        Tuple tuple = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
        tuple.setField(0, new IntField(nTuplesDeleted));
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
