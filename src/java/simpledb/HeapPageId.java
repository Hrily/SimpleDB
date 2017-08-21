package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {
    
    int tableId, pageNo;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @author hrily
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        this.tableId = tableId;
        this.pageNo = pgNo;
    }

    /**
     * @author hrily
     * @return the table associated with this PageId 
     */
    public int getTableId() {
        return tableId;
    }

    /**
     * @author hrily
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() {
        return pageNo;
    }

    /**
     * @author hrily
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        String hash = String.valueOf(tableId) + String.valueOf(pageNo);
        return hash.hashCode();
    }

    /**
     * Compares one PageId to another.
     *
     * @author hrily
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        if(o == null || !o.getClass().equals(this.getClass()))
            return false;
        HeapPageId heapPageId = (HeapPageId) o;
        if(heapPageId.tableId != this.tableId)
            return false;
        if(heapPageId.pageNo != this.pageNo)
            return false;
        return true;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];
        data[0] = getTableId();
        data[1] = pageNumber();
        return data;
    }

}
