package simpledb;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbField, aField;
    private Op op;
    private Type gbFieldType;
    private TupleDesc tupleDesc;
    
    private HashMap<Field, Tuple> fieldTupleMap;

    /**
     * Aggregate constructor
     * 
     * @author hrily
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.fieldTupleMap = new HashMap<Field, Tuple>();
        if(!this.op.equals(Aggregator.Op.COUNT))
            throw new IllegalArgumentException();
        if(gbfield == NO_GROUPING)
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        else
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * 
     * @author hrily
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        int index = (gbField != NO_GROUPING) ? 1 : 0;
        Field field = (gbField != NO_GROUPING)
                ? tup.getField(gbField)
                : noGroupField;
        if(!fieldTupleMap.containsKey(field)){
            fieldTupleMap.put(field, new Tuple(tupleDesc));
            fieldTupleMap.get(field).setField(0, field);
            fieldTupleMap.get(field).setField(index, new IntField(0));
        }
        IntField field1 = (IntField) fieldTupleMap.get(field).getField(index);
        int result = field1.getValue() + 1;
        fieldTupleMap.get(field).setField(
                    index, new IntField(result));
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @author hrily
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new Aggregator.TupleMapIterator(fieldTupleMap, tupleDesc);
    }

}
