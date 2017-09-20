package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * LockManager is class which manages locks for transactions.
 * It stores states of various locks on pages and provides atomic grant
 * and release of locks.
 * @author hrishi
 */
public class LockManager {
    
    private HashMap<PageId, Set<TransactionId>> readLocks;
    private HashMap<PageId, TransactionId> writeLock;
    private HashMap<TransactionId, Set<PageId>> sharedPages;
    private HashMap<TransactionId, Set<PageId>> exclusivePages;

    public LockManager() {
        readLocks = new HashMap<PageId, Set<TransactionId>>();
        writeLock = new HashMap<PageId, TransactionId>();
        sharedPages = new HashMap<TransactionId, Set<PageId>>();
        exclusivePages = new HashMap<TransactionId, Set<PageId>>();
    }
    
    /**
     * Checks if transaction has lock on a page
     * @param tid Transaction Id
     * @param pid Page Id
     * @return boolean True if holds lock
     */
    public boolean holdsLock(TransactionId tid, PageId pid){
        if(readLocks.containsKey(pid) && readLocks.get(pid).contains(tid))
            return true;
        if(writeLock.containsKey(pid) && writeLock.get(pid).equals(tid))
            return true;
        return false;
    }
    
    private void addLock(TransactionId tid, PageId pid, Permissions pm){
        if(pm.equals(Permissions.READ_ONLY)){
            if(!readLocks.containsKey(pid))
                readLocks.put(pid, new HashSet<TransactionId>());
            readLocks.get(pid).add(tid);
            if(!sharedPages.containsKey(tid))
                sharedPages.put(tid, new HashSet<PageId>());
            sharedPages.get(tid).add(pid);
            return;
        }
        writeLock.put(pid, tid);
        if(!exclusivePages.containsKey(tid))
            exclusivePages.put(tid, new HashSet<PageId>());
        exclusivePages.get(tid).add(pid);
    }
    
    /**
     * Grants lock to the Transaction.
     * @param tid TransactionId requesting lock.
     * @param pid PageId on which the lock is requested.
     * @param pm The type of permission.
     * @return boolean True if lock is successfully granted.
     */
    public synchronized boolean grantLock(TransactionId tid, PageId pid, 
            Permissions pm){
        // If Page requested is new and not in lock
        if( ( !readLocks.containsKey(pid) || readLocks.get(pid).isEmpty() ) && 
                !writeLock.containsKey(pid)){
            // We can grant any kind of lock
            addLock(tid, pid, pm);
            return true;
        }
        // Page in lock
        // If requested permission is read
        if(pm.equals(Permissions.READ_ONLY)){
            // If page permission is read write 
            // then we cannot give the lock before release of write lock.
            if(writeLock.containsKey(pid) && writeLock.get(pid) != tid)
                return false;
            // Else the page permission is read 
            // we can give the lock.
            addLock(tid, pid, pm);
            return true;
        }
        // Requested permission is write.
        // This lock can only be given iff there is no other transaction with 
        // any kind of lock on that page. But this can't happen as we
        // handled that case above.
        // This means we can only give lock iff tid is only transaction on the
        // requested page
        if(readLocks.containsKey(pid) && 
                readLocks.get(pid).contains(tid) &&
                readLocks.get(pid).size() == 1){
            addLock(tid, pid, pm);
            return true;
        }
        // But if transaction tid has already a write lock
        // then the result is already success
        if(exclusivePages.containsKey(tid) &&
                exclusivePages.get(tid).contains(pid))
            return true;
        // In all other cases we fail to grant lock.
        return false;
    }
    
    /**
     * Releases locks associated with given transaction and page.
     * @param tid The TransactionId.
     * @param pid The PageId.
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid){
        if(readLocks.containsKey(pid))
            readLocks.get(pid).remove(tid);
        writeLock.remove(pid);
        if(sharedPages.containsKey(tid))
            sharedPages.get(tid).remove(pid);
        if(exclusivePages.containsKey(tid))
            exclusivePages.get(tid).remove(pid);
    }
    
    /**
     * Releases Lock related to a page
     * @param pid PageId
     */
    public synchronized void removePage(PageId pid){
        readLocks.remove(pid);
        writeLock.remove(pid);
    }
    
    /**
     * Releases all pages associated with given Transaction.
     * @param tid The TransactionId.
     */
    public void releaseAllPages(TransactionId tid){
        if(sharedPages.containsKey(tid)){
            for(PageId pid: sharedPages.get(tid))
                readLocks.remove(pid);
            sharedPages.remove(tid);
        }
        if(exclusivePages.containsKey(tid)){
            for(PageId pid: exclusivePages.get(tid))
                writeLock.remove(pid);
            exclusivePages.remove(tid);
        }
    }
    
}
