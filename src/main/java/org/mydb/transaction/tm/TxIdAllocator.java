package org.mydb.transaction.tm;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/16 21:48
 */
public class TxIdAllocator {
    private AtomicInteger txId;
    public TxIdAllocator(){
        txId = new AtomicInteger(0);
    }
    public int getNextTxId(){
        return txId.getAndAdd(1);
    }
}
