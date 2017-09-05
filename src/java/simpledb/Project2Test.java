/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb;

import java.io.File;

/**
 *
 * @author hrishi
 */
public class Project2Test {
    
    public static String FILE1 = "/home/hrishi/git/SimpleDB/test/simpledb/sample_data/data1.dat";
    public static String FILE2 = "/home/hrishi/git/SimpleDB/test/simpledb/sample_data/data2.dat";
    
    public static void main(String[] argv) {
        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };

        TupleDesc td = new TupleDesc(types, names);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File(FILE1), td);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File(FILE2), td);
        Database.getCatalog().addTable(table2, "t2");

        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition
        Filter sf1 = new Filter(
                                new Predicate(0,
                                Predicate.Op.GREATER_THAN, new IntField(1)),  ss1);

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);

        // and run it
        try {
            long start = System.currentTimeMillis();
            int lines = 0;
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
                lines++;
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);
            System.out.println("\n" + lines + " rows, " 
                    + String.valueOf(System.currentTimeMillis() - start) + "ms.\n");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
