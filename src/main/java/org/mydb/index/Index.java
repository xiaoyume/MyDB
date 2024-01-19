package org.mydb.index;

import org.mydb.meta.Tuple;

public interface Index {
    /**
     * 查询
     * @param key
     * @return
     */
    Tuple get(Tuple key);

    /**
     * 移除
     * @param key
     * @return
     */
    boolean remove(Tuple key);

    /**
     * 插入
     * @param tuple
     */
    void insert(Tuple tuple);
}
