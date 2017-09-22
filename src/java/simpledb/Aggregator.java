package simpledb;

import java.io.Serializable;
import java.util.HashMap;
import java.util.NoSuchElementException;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public interface Aggregator extends Serializable {
    static final int NO_GROUPING = -1;
    
    public static Field noGroupField = new IntField(Integer.MIN_VALUE);

    public enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
        
        public String toString()
        {
        	if (this==MIN)
        		return "min";
        	if (this==MAX)
        		return "max";
        	if (this==SUM)
        		return "sum";
        	if (this==AVG)
        		return "avg";
        	if (this==COUNT)
        		return "count";
        	throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup);

    /**
     * Create a DbIterator over group aggregate results.
     * @see simpledb.TupleIterator for a possible helper
     */
    public DbIterator iterator();
    
    
    /**
     * TupleMapIterator is class which converts HashMap &lt; Field, Tuple &gt;
     * to a DbIterator.
     * 
     * @author hrily
     * @see DbIterator
     */
    public static class TupleMapIterator implements DbIterator {
        
        HashMap<Field, Tuple> fieldTupleMap;
        TupleDesc tupleDesc;
        Field[] keyArray;
        int position;

        public TupleMapIterator(HashMap<Field, Tuple> fieldTupleMap, TupleDesc tupleDesc) {
            this.fieldTupleMap = fieldTupleMap;
            this.tupleDesc = tupleDesc;
            this.keyArray = fieldTupleMap
                    .keySet()
                    .toArray(new Field[fieldTupleMap.size()]);
            this.position = -1;
        }

        public void open() throws DbException, TransactionAbortedException {
            position = 0;
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(position < 0)
                throw new DbException("Iterator not opened...");
            return position < keyArray.length;
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(position < 0)
                throw new DbException("Iterator not opened...");
            if(!this.hasNext())
                return null;
            return fieldTupleMap.get(keyArray[position++]);
        }

        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        public void close() {
            position = -1;
        }
        
    }
    
}
