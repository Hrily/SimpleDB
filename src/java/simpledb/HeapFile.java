package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
    
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @author hrily
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     * @param td 
     *            the tuple desc of the file
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @author hrily
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that eachfor
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * HeapFile has a "unique id," and that you always return the same value 
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @author hrily
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @author hrily
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * Reads page from the file with given id
     * 
     * @author hrily
     * @param pid Page ID
     * @return Page the read page
     */
    public Page readPage(PageId pid) {
        // Offset to seek
        int offset = BufferPool.PAGE_SIZE * pid.pageNumber();
        byte[] data = new byte[BufferPool.PAGE_SIZE];
        try{
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(offset);
            raf.read(data);
            return new HeapPage((HeapPageId) pid, data);
        }catch(FileNotFoundException fnfe){
            fnfe.printStackTrace();
        }catch(IOException ioe){
            ioe.printStackTrace();
        }
        return null;
    }

    /**
     * Write given page to disk
     * 
     * @author hrily
     * @param page Page to write
     * @throws IOException 
     */
    public void writePage(Page page) throws IOException {
        byte[] data = page.getPageData();
        int offset = BufferPool.PAGE_SIZE * page.getId().pageNumber();
        try{
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.seek(offset);
            raf.write(data);
        }catch(IOException ioe){
            ioe.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     * 
     * @author hrily
     * @return The number of pages
     */
    public int numPages() {
        return (int) Math.ceil((double) file.length() / BufferPool.PAGE_SIZE);
    }

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @author hrily
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pages = new ArrayList<Page>();
        for(int i = 0; i < numPages(); i++){
            PageId pid = new HeapPageId(this.getId(), i);
            HeapPage page 
                = (HeapPage) Database
                    .getBufferPool()
                    .getPage(tid, pid, Permissions.READ_ONLY);
            if(page.getNumEmptySlots() > 0){
                page = (HeapPage) Database
                    .getBufferPool()
                    .getPage(tid, pid, Permissions.READ_WRITE);
                page.insertTuple(t);
                pages.add(page);
                break;
            }
        }
        if(pages.isEmpty()){
            HeapPageId pid = new HeapPageId(this.getId(), this.numPages());
            HeapPage heapPage = new HeapPage(pid, HeapPage.createEmptyPageData());
            heapPage.insertTuple(t);
            this.writePage(heapPage);
            pages.add(heapPage);
        }
        return pages;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return page;
    }

    /**
     * Returns iterator to all the tuples in this file
     * 
     * @author hrily
     * @param tid
     * @return DBFileIterator iterator to the tuples
     */
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this.getId(), this.numPages());
    }
    
    /**
     * Class for iterating over all tuples of this file
     * 
     * @see simpleddb.DbFileIterator
     * @author hrily
     */
    public class HeapFileIterator implements DbFileIterator {
        
        TransactionId tid;
        int pageCounter;
        int tableId;
        int numPages;
        Page page;
        Iterator<Tuple> tuples;
        HeapPageId pid;
        
        /**
         * Constructor for Iterator
         * 
         * @param tid TransactionId of requesting transaction
         * @param tableId id of the HeapFile
         * @param numPages number of pages in file
         */
        public HeapFileIterator(TransactionId tid, int tableId, int numPages) {
            this.tid = tid;
            this.pageCounter = 0;
            this.tableId = tableId;
            this.numPages = numPages;
        }
        
        /**
         * Gets tuples from file with given page number
         * 
         * @param pageNumber
         * @return Iterator<Tuples> Iterator for tuples in the page
         * @throws DbException
         * @throws TransactionAbortedException 
         */
        private Iterator<Tuple> getTuples(int pageNumber) throws  DbException, TransactionAbortedException {
            pid = new HeapPageId(tableId, pageNumber);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return heapPage.iterator();
        }

        /**
         * Opens it iterator for iteration
         * 
         * @throws DbException
         * @throws TransactionAbortedException 
         */
        public void open() throws DbException, TransactionAbortedException {
            pageCounter = 0;
            tuples = getTuples(pageCounter);
        }

        /**
         * Checks if iterator has next tuple
         * 
         * @return boolean true if exists
         * @throws DbException
         * @throws TransactionAbortedException 
         */
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // If there are no tuples
            if(tuples == null)
                return false;
            // Check if tuple has next
            if(tuples.hasNext())
                return true;
            // Check if all pages are iterated
            if(pageCounter + 1 >= numPages)
                return false;
            // Else check if there is next page
            // If Page is exhausted get new page tuples
            while(pageCounter + 1 < numPages && !tuples.hasNext()){
                // Release the current page
                Database.getBufferPool().releasePage(tid, pid);
                // Get tuples of next page
                pageCounter++;
                tuples = getTuples(pageCounter);
            }
            return this.hasNext();
        }

        /**
         * Get next tuple in this file 
         * 
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         * @throws NoSuchElementException 
         */
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // If there are no tuples, throw exception
            if(tuples == null) 
                throw new NoSuchElementException();
            return tuples.next();
        }

        /**
         * Rewinds iterator to the start of file
         * 
         * @throws DbException
         * @throws TransactionAbortedException 
         */
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Close the iterator
         */
        public void close() {
            // Release the  current page
            Database.getBufferPool().releasePage(tid, pid);
            tuples = null;
            pid = null;
        }
        
    }

}

