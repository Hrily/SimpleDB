package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private int field1, field2;
    private Predicate.Op op;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @author hrily
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        this.field1 = field1;
        this.field2 = field2;
        this.op = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @author hrily
     * @param t1
     *            First tuple
     * @param t2
     *            Second tuple
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        return t1.getField(field1).compare(op, t2.getField(field2));
    }
    
    /**
     * @author hrily
     * @return First field
     */
    public int getField1() {
        return field1;
    }
    
    /**
     * @author hrily
     * @return First field
     */
    public int getField2() {
        return field2;
    }
    
    /**
     * @author hrily
     * @return The predicate operator
     */
    public Predicate.Op getOperator() {
        return op;
    }

    public List<Tuple> applyAll(DbIterator iterator1, DbIterator iterator2) 
            throws DbException, TransactionAbortedException{
        iterator1.open();
        iterator2.open();
        List<Tuple> tupleList = new ArrayList<Tuple>();
        List<Tuple> list2 = Tuple.sortedList(iterator2, this.getField2());
        while(iterator1.hasNext()){
            List<Tuple> list = this.applyOne(iterator1.next(), list2);
            if(list != null)
                tupleList.addAll(list);
        }
        iterator1.close();
        iterator2.close();
        return tupleList;
    }
    
    private List<Tuple> applyOne(Tuple tuple, List<Tuple> tuples){
        if(tuples.isEmpty()) 
            return null;
        boolean found = true;
        List<Tuple> list = new ArrayList<Tuple>();
        Tuple searchTuple = new Tuple(tuples.get(0).getTupleDesc());
        searchTuple.setField(field2, tuple.getField(field1));
        int index = Collections.binarySearch(tuples, searchTuple, new TupleComparator(field2, true));
        if(index < 0){
            index = (-index) + 1;
            found = false;
        }
        int startIndex = 0;
        JoinPredicate eqPredicate = new JoinPredicate(field1, Predicate.Op.EQUALS, field2);
        switch(op){
            case EQUALS:
            case LESS_THAN_OR_EQ:
                    while(found && index >= 0
                            && eqPredicate.filter(tuple, tuples.get(index)))
                        index--;
                    startIndex = index + 1;
                    break;
            case LESS_THAN:
                    while(found && index < tuples.size() 
                            && eqPredicate.filter(tuple, tuples.get(index)))
                        index++;
                    startIndex = index;
                    break;
        }
        int i = startIndex;
        while(  i < tuples.size() 
                && ( this.filter(tuple, tuples.get(i))
                     || op.equals(Predicate.Op.NOT_EQUALS)) ){
            if(this.filter(tuple, tuples.get(i))){
                list.add(Tuple.merge(tuple, tuples.get(i)));
            }
            i++;
        }
        return list;
    }
}
