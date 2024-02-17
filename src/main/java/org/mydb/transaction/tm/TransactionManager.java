package org.mydb.transaction.tm;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/16 21:49
 */
public class TransactionManager {
    private static TransactionManager manager;
    private TxIdAllocator txIdAllocator;
    static {
        manager = new TransactionManager();
    }
    private TransactionManager() {
        txIdAllocator = new TxIdAllocator();
    }
    public int getNextTxId() {
        return txIdAllocator.getNextTxId();
    }
    public static TransactionManager getInstance(){
        return manager;
    }
}
