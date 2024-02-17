package org.mydb.transaction.tm;

import org.mydb.index.BaseIndex;
import org.mydb.meta.Relation;
import org.mydb.meta.Tuple;
import org.mydb.store.item.Item;
import org.mydb.transaction.rm.LogRecord;
import org.mydb.transaction.rm.TransOPAction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/16 21:52
 */
public class Transaction {
    private int txId;
    private Relation relation;
    private TransactionManager manager = TransactionManager.getInstance();
    //保存事务级别的unRedo
    //简单版本的事务
    private List<LogRecord> unRedoLog = new ArrayList<>();
    private int state = TransStateConst.NOT_IN_TRANSACTION;

    //开启事务
    public int begin() {
        txId = manager.getNextTxId();
        state = TransStateConst.IN_TRANSACTION;
        return txId;
    }

    //提交
    public void commit() {
        //flush log
        state = TransStateConst.COMMITTED;
        unRedoLog.clear();
    }

    //
    public void rollback() {
        state = TransStateConst.ROLLBACK;
        //从后往前undo
        for (int i = unRedoLog.size(); i >= 0; i--) {
            undo(unRedoLog.get(i));
        }
        unRedoLog.clear();
    }

    public int getTxId() {
        return txId;
    }

    public void insertLog(LogRecord record) {
        unRedoLog.add(record);
    }

    public void undo(LogRecord record) {
        //记录undo
        switch (record.getOperation()) {
            case TransOPAction.DELETE -> {
                relation.insert(new Item(record.getAfter()));
            }
            case TransOPAction.INSERT -> {
                relation.delete(record.getAfter());
            }
            case TransOPAction.UPDATE -> {
                relation.delete(record.getAfter());
                relation.insert(record.getBefore());
            }
        }

        //key相同的情况
        //方法，每条记录有一个唯一的记录id
        //插入了记录，在索引里也要移除
        //索引级别的redo
        for (BaseIndex index : relation.getIndexs()) {
            Tuple key = index.convertToKey(record.getAfter());
            switch (record.getOperation()) {
                case TransOPAction.INSERT -> {
                    index.removeOne(key);
                }
                case TransOPAction.DELETE -> {
                    index.insert(key, index.isUnique());
                }
                case TransOPAction.UPDATE -> {
                    index.removeOne(key);
                    Tuple beforeKey = index.convertToKey(record.getBefore());
                    index.insert(beforeKey, index.isUnique());
                }
            }
        }
    }

    public int getState(){
        return state;
    }
    public Transaction setState(int state) {
        this.state = state;
        return this;
    }
    public Transaction setTxId(int txId) {
        this.txId = txId;
        return this;
    }

}
