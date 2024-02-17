package org.mydb.transaction.tm;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 事务常量
 * @date 2024/2/16 21:45
 */
public class TransStateConst {
    public static final Integer NOT_IN_TRANSACTION = 0;
    public static final Integer IN_TRANSACTION = 1;
    public static final Integer ROLLBACK = 2;
    public static final Integer COMMITTED = 3;
}
