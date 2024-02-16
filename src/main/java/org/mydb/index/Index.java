package org.mydb.index;

import org.mydb.index.bp.GetRes;
import org.mydb.meta.Tuple;

import java.util.List;

public interface Index {
    List<Tuple> getAll(Tuple tuple);//查询所有key
    GetRes getFirst(Tuple key);//查询第一个key
    int remove(Tuple key);//移除
    boolean removeOne(Tuple key);
    void insert(Tuple key, boolean isUnique);//插入
}
