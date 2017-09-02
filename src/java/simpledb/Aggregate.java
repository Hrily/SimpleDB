package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    
    private DbIterator child, iterator;
    private int aField, gbField;
    private Aggregator.Op op;
    private TupleDesc tupleDesc;
    private Aggregator aggregator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * @author hrily
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	this.child = child;
        this.aField = afield;
        this.gbField = (gfield != -1) ? gfield : Aggregator.NO_GROUPING;
        this.op = aop;
        this.tupleDesc = child.getTupleDesc();
        Type groupType = (gfield != -1) ? tupleDesc.getFieldType(gfield) : null;
        if(this.tupleDesc.getFieldType(afield) == Type.INT_TYPE)
            aggregator = new IntegerAggregator(gbField, groupType, afield, aop);
        else
            aggregator = new StringAggregator(gbField, groupType, afield, aop);
    }

    /**
     * @author hrily
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	return gbField;
    }

    /**
     * @author hrily
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        if(gbField == Aggregator.NO_GROUPING)
            return null;
        return tupleDesc.getFieldName(gbField);
    }

    /**
     * @author hrily
     * @return the aggregate field
     * */
    public int aggregateField() {
        return aField;
    }

    /**
     * @author hrily
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return tupleDesc.getFieldName(aField);
    }

    /**
     * @author hrily
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    /**
     * Opens the iterator
     * 
     * @author hrily
     * @throws NoSuchElementException
     * @throws DbException
     * @throws TransactionAbortedException 
     */
    @Override
    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        super.open();
	child.open();
        while(child.hasNext())
            aggregator.mergeTupleIntoGroup(child.next());
        iterator = aggregator.iterator();
        iterator.open();
        child.close();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     * 
     * @author hrily
     * @throws DbException
     * @throws TransactionAbortedException
     * @return Tuple The next tuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	if(iterator == null)
            throw new DbException("Operator not opened!");
        return iterator.next();
    }

    /**
     * Rewinds the iterator
     * 
     * @author hrily
     * @throws DbException
     * @throws TransactionAbortedException 
     */
    public void rewind() throws DbException, TransactionAbortedException {
	iterator = aggregator.iterator();
        iterator.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * child.
     * 
     * @author hrily
     * @return TupleDesc The TupleDesc of Aggregrate
     */
    public TupleDesc getTupleDesc() {
        TupleDesc atd;
        StringBuilder agFieldName = new StringBuilder();
        agFieldName
                .append(op.toString())
                .append("(")
                .append(tupleDesc.getFieldName(aField))
                .append(")");
        if(gbField == Aggregator.NO_GROUPING)
            atd = new TupleDesc(
                    new Type[]{Type.INT_TYPE},
                    new String[]{agFieldName.toString()}
                );
        else 
            atd = new TupleDesc(
                    new Type[]{tupleDesc.getFieldType(gbField), Type.INT_TYPE},
                    new String[]{tupleDesc.getFieldName(gbField), agFieldName.toString()}
                );
	return atd;
    }

    /**
     * Closes the iterator
     * @author hrily
     */
    @Override
    public void close() {
	iterator = null;
        super.close();
    }

    @Override
    public DbIterator[] getChildren() {
	return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
	child = children[0];
    }
    
}
