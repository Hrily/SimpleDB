package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    
    private Predicate predicate;
    private DbIterator iterator;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @author hrily
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.predicate = p;
        this.iterator = child;
    }

    /**
     * @author hrily
     * @return Predicate
     */
    public Predicate getPredicate() {
        return predicate;
    }

    /**
     * @author hrily
     * @return The tuple descriptor of the tuples
     */
    public TupleDesc getTupleDesc() {
        return iterator.getTupleDesc();
    }

    /**
     * Opens the iterator for scanning
     * 
     * @author hrily
     * @throws DbException
     * @throws NoSuchElementException
     * @throws TransactionAbortedException 
     */
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        iterator.open();
    }

    /**
     * Closes the iterator
     * @author hrily
     */
    public void close() {
        iterator.close();
        super.close();
    }

    /**
     * Rewinds the iterator
     * 
     * @author hrily
     * @throws DbException
     * @throws TransactionAbortedException 
     */
    public void rewind() throws DbException, TransactionAbortedException {
        iterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @author hrily
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        Tuple tuple = iterator.hasNext() 
                    ? iterator.next()
                    : null;
        while(tuple != null && !predicate.filter(tuple))
            tuple = iterator.hasNext() 
                    ? iterator.next()
                    : null;
        return tuple;
    }

    /**
     * @author hrily
     * @return return the children DbIterators of this operator. If there is
     *         only one child, return an array of only one element. For join
     *         operators, the order of the children is not important. But they
     *         should be consistent among multiple calls.
     */
    @Override
    public DbIterator[] getChildren() {
        DbIterator[] dbIterators = {iterator};
        return dbIterators;
    }

    /**
     * Set the children(child) of this operator. If the operator has only one
     * child, children[0] should be used. If the operator is a join, children[0]
     * and children[1] should be used.
     * 
     * @author hrily
     * @param children
     *            the DbIterators which are to be set as the children(child) of
     *            this operator
     * */
    @Override
    public void setChildren(DbIterator[] children) {
        iterator = children[0];
    }

}
