package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private PageId pid;
    int tupleNo;
    
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @author hrily
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pid = pid;
        this.tupleNo = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        return tupleNo;
    }

    /**
     * @author hrily
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @author hrily
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if(!o.getClass().equals(this.getClass()))
            return false;
        RecordId recordId = (RecordId) o;
        return this.pid.equals(recordId.pid) && this.tupleNo == recordId.tupleNo;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @author hrily
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return Integer.parseInt(String.valueOf(pid.hashCode()) + String.valueOf(tupleNo));
    }

}