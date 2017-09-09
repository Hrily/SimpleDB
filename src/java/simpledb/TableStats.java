package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;
    
    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    private int tableId;
    private int ioCostPerPage;
    private HeapFile file;
    private DbFileIterator iterator;
    
    private Tuple minTuple, maxTuple;
    private int numTuples;
    
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        file = (HeapFile) Database.getCatalog().getDbFile(tableid);
        Transaction transaction = new Transaction();
        iterator = file.iterator(transaction.getId());
        minTuple = new Tuple(file.getTupleDesc());
        maxTuple = new Tuple(file.getTupleDesc());
        try {
            // Compute min max
            // Init
            iterator.open();
            Tuple tuple = iterator.next();
            for(int i=0; i<tuple.getTupleDesc().numFields(); i++){
                minTuple.setField(i, tuple.getField(i));
                maxTuple.setField(i, tuple.getField(i));
            }
            numTuples++;
            // Compute
            while(iterator.hasNext()){
                Tuple t = iterator.next();
                for(int i=0; i<t.getTupleDesc().numFields(); i++){
                    if(minTuple.getField(i)
                            .compare(Predicate.Op.GREATER_THAN, t.getField(i)))
                        minTuple.setField(i, t.getField(i));
                    if(maxTuple.getField(i)
                            .compare(Predicate.Op.LESS_THAN, t.getField(i)))
                        maxTuple.setField(i, t.getField(i));
                }
                numTuples++;
            }
        }catch (DbException e){
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
    }

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            Logger.getLogger(TableStats.class.getName()).log(Level.SEVERE, null, e);
        } catch (SecurityException e) {
            Logger.getLogger(TableStats.class.getName()).log(Level.SEVERE, null, e);
        } catch (IllegalArgumentException e) {
            Logger.getLogger(TableStats.class.getName()).log(Level.SEVERE, null, e);
        } catch (IllegalAccessException e) {
            Logger.getLogger(TableStats.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return file.numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) Math.ceil( selectivityFactor * this.totalTuples() );
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * @return The averege selectivity
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        int nbins = Math.min( NUM_HIST_BINS, 
            ((IntField) maxTuple.getField(field)).getValue() );
        if(file.getTupleDesc().getFieldType(field).equals(Type.INT_TYPE)){
            IntHistogram hist = new IntHistogram(
                    nbins,  
                    ((IntField) minTuple.getField(field)).getValue(), 
                    ((IntField) maxTuple.getField(field)).getValue() );
            try {
                iterator.rewind();
                while(iterator.hasNext()){
                    Tuple tuple = iterator.next();
                    hist.addValue(
                            ((IntField) tuple.getField(field)).getValue());
                }
            } catch (DbException ex) {
                Logger.getLogger(TableStats.class.getName()).log(Level.SEVERE, null, ex);
            } catch (TransactionAbortedException ex) {
                Logger.getLogger(TableStats.class.getName()).log(Level.SEVERE, null, ex);
            }
            return hist.estimateSelectivity(op, 
                    ((IntField) constant).getValue());
        }
        StringHistogram hist = new StringHistogram(NUM_HIST_BINS);
        try {
            iterator.rewind();
            while(iterator.hasNext()){
                Tuple tuple = iterator.next();
                hist.addValue(
                        ((StringField) tuple.getField(field)).getValue());
            }
        } catch (DbException ex) {
            Logger.getLogger(TableStats.class.getName()).log(Level.SEVERE, null, ex);
        } catch (TransactionAbortedException ex) {
            Logger.getLogger(TableStats.class.getName()).log(Level.SEVERE, null, ex);
        }
        return hist.estimateSelectivity(op, 
                ((StringField) constant).getValue());
    }

    /**
     * @return 
     *      return the total number of tuples in this table
     */
    public int totalTuples() {
        return numTuples;
    }

}
