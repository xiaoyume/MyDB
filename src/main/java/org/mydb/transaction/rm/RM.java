package org.mydb.transaction.rm;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/16 20:25
 */
public interface RM {
    //返回lsn
    int prepare();
    void undo(int lsn);
    void redo(int lsn);
    //TM重启的时候，给rm传递lsn
    void tmStartUp(int lsn);
}
