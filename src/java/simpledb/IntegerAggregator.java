package simpledb;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private class MyInteger {
        
        int res, cnt;
        Op op;

        public MyInteger(Op op) {
            this.res = 0;
            this.op = op;
            if(op.equals(Aggregator.Op.MIN)) res = Integer.MAX_VALUE;
            if(op.equals(Aggregator.Op.MAX)) res = Integer.MIN_VALUE;
        }
        
        public void add(int n){
            switch (op) {
                case MAX:
                    res = Math.max(res, n);
                    return;
                case MIN:
                    res = Math.min(res, n);
                    return;
                case SUM:
                    res += n;
                    return;
                case AVG:
                    res += n;
                    cnt++;
                    return;
                case COUNT:
                    res++;
                    return;
                default:
                    throw new AssertionError();
            }
        }
        
        public int get(){
            if(op.equals(Aggregator.Op.AVG))
                return res / cnt;
            return res;
        }
        
    }
    
    private int gbField, aField;
    private Type gbFieldType;
    private Op op;
    
    private HashMap<Field, Tuple> fieldTupleMap;
    private HashMap<Field, MyInteger> fieldResMap;
    private TupleDesc tupleDesc;
    private Tuple noGroupTuple;
            
    /**
     * Aggregate constructor
     * 
     * @author hrily
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.fieldTupleMap = new HashMap<Field, Tuple>();
        this.fieldResMap = new HashMap<Field, MyInteger>();
        if(gbfield == NO_GROUPING){
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple noGroupTuple = new Tuple(tupleDesc);
            noGroupTuple.setField(0, new IntField(0));
            fieldResMap.put(noGroupField, new MyInteger(op));
            fieldTupleMap.put(noGroupField, noGroupTuple);
        }else
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @author hrily
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        int index = (gbField != NO_GROUPING) ? 1 : 0;
        Field field = (gbField != NO_GROUPING)
                ? tup.getField(gbField)
                : noGroupField;
        if(!fieldTupleMap.containsKey(field)){
            fieldResMap.put(field, new MyInteger(op));
            fieldTupleMap.put(field, new Tuple(tupleDesc));
            fieldTupleMap.get(field).setField(0, field);
            fieldTupleMap.get(field).setField(index, new IntField(0));
        }
        IntField field2 = (IntField) tup.getField(aField);
        fieldResMap.get(field).add(field2.getValue());
        int result = fieldResMap.get(field).get();
        fieldTupleMap.get(field).setField(
                    index, new IntField(result));
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @author hrily
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        return new Aggregator.TupleMapIterator(fieldTupleMap, tupleDesc);
    }

}
