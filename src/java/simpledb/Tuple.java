package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private TupleDesc tupleDesc;
    private RecordId recordId;
    List<Field> fieldValues;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @author hrily
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.tupleDesc = td;
        fieldValues = new ArrayList<Field>();
        for(int i = 0; i < td.numFields(); i++)
            fieldValues.add(null);
    }

    /**
     * @author hrily
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @author hrily
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @author hrily
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @author hrily
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        fieldValues.set(i, f);
    }

    /**
     * @author hrily
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fieldValues.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     * 
     * @author hrily
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for(Field field: fieldValues)
            stringBuilder.append(field.toString()).append("\t");
        return stringBuilder.append("\n").toString();
    }
    
    /**
     * @author hrily
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        return fieldValues.iterator();
    }
    
    /**
     * Merges two tuples
     * @param t1 Tuple 1
     * @param t2 Tuple 2
     * @return tuple The merged Tuple
     */
    public static Tuple merge(Tuple t1, Tuple t2) {
        Tuple tuple = new Tuple(
                TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc()));
        tuple.fieldValues = new ArrayList<Field>();
        tuple.fieldValues.addAll(t1.fieldValues);
        tuple.fieldValues.addAll(t2.fieldValues);
        return tuple;
    }
    
    /**
     * Removes a field from the tuple 
     * Also chages the tuple desc accordingly
     * @param i The index of field to remove
     */
    public void removeField(int i){
        fieldValues.remove(i);
        tupleDesc.removeField(i);
    }

    @Override
    public boolean equals(Object o) {
        if(o == null)
            return false;
        if(o.getClass() != this.getClass())
            return false;
        Tuple tuple = (Tuple) o;
        if(this.fieldValues.size() != tuple.fieldValues.size())
            return false;
        for(int i = 0; i < this.fieldValues.size(); i++)
            if(!this.fieldValues.get(i).equals(
                    tuple.fieldValues.get(i)))
                return false;
        return true;
    }

    @Override
    public int hashCode() {
        return fieldValues.hashCode();
    }
    
}