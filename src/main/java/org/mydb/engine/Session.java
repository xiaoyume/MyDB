package org.mydb.engine;

import org.mydb.index.BaseIndex;
import org.mydb.meta.Relation;
import org.mydb.meta.Tuple;
import org.mydb.transaction.rm.LogRecord;
import org.mydb.transaction.tm.TransStateConst;
import org.mydb.transaction.tm.Transaction;
import org.mydb.transaction.rm.TransOPAction;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/17 11:10
 */
public class Session {
    private ThreadLocal<Transaction> local = new ThreadLocal<>();

    /**
     * 执行计划
     *
     * @param operation
     * @param relation
     * @param tupleBefore
     * @param tupleAfter
     */
    public void execute(int operation, Relation relation, Tuple tupleBefore,
                        Tuple tupleAfter) {
        precheck();
        boolean isNotInTransaction = (getState() == TransStateConst.NOT_IN_TRANSACTION);
        if (isNotInTransaction) {
            begin();
        }
        try {
            switch (operation) {
                case TransOPAction.INSERT -> {
                    insert(relation, tupleAfter);
                }
                case TransOPAction.DELETE -> {
                    delete(relation, tupleBefore);
                }
                case TransOPAction.UPDATE -> {
                }
            }
            if(isNotInTransaction){
                commit();
            }
        }catch (Exception e){
            rollback();
        }
    }

    /**
     * 通过tuple里的pageNo,countno来唯一确定一个tuple
     * @param relation
     * @param tuple
     */
    public void delete(Relation relation, Tuple tuple){
        relation.delete(tuple);
        for(BaseIndex baseIndex : relation.getIndexs()){
            baseIndex.remove(tuple);
        }
        //
        getTransaction().insertLog(new LogRecord(getTxId(), TransOPAction.DELETE, tuple, null));
    }

    private void rollback() {
        precheck();
        local.get().rollback();
        local.remove();
    }

    private void commit() {
        precheck();
        local.get().rollback();
        local.remove();
    }

    private void insert(Relation relation, Tuple tupleAfter) {
        relation.insert(tupleAfter);
        //插入索引
        for(BaseIndex baseIndex : relation.getIndexs()){
            baseIndex.insert(tupleAfter, baseIndex.isUnique());
        }
        //插入日志
        getTransaction().insertLog(new LogRecord(getTxId(), TransOPAction.DELETE, tupleAfter, null));
    }

    public void update(Relation relation, Tuple tupleBefore, Tuple tupleAfter) {
        relation.update(tupleBefore, tupleAfter);
    }

    private int getTxId() {
        precheck();
        return local.get().getTxId();
    }

    /**
     * 开启事务
     * @return 返回事务状态
     */
    private void begin() {
        precheck();
        local.get().begin();
    }

    private int getState() {
        precheck();
        return local.get().getState();
    }

    /**
     * 预先检查线程里有没有事务，如果没有就创建一个
     */
    private void precheck() {
        if(local.get() == null){
            Transaction transaction = new Transaction();
            local.set(transaction);
        }
    }

    public Transaction getTransaction(){
        precheck();
        return local.get();
    }
}
