package org.mydb.transaction.tm;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 事务上下文
 * @date 2024/2/16 21:43
 */
public class TransContext {
    //事务id
    private int txId;
    //当前事务状态
    private Integer state;
    public TransContext(){
        //默认不开启事务
        state = TransStateConst.NOT_IN_TRANSACTION;

    }
}
