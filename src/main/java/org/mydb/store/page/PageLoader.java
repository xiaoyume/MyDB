package org.mydb.store.page;

import org.mydb.meta.Tuple;
import org.mydb.store.item.ItemPointer;
import org.mydb.store.page.Page;
import org.mydb.store.page.PageHeaderData;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 存储page中的所有tuple
 * @date 2023/12/11 20:38
 */
public class PageLoader {
    Page page;
    private Tuple[] tuples;
    private int tupleCount;
    public PageLoader(Page page) {
        this.page = page;
    }

    /**
     * 从page中读取tuples
     */
    public void load(){
        PageHeaderData pageHeaderData = PageHeaderData.read(page);
        tupleCount = pageHeaderData.getTupleCount();
        int ptrStartOff = pageHeaderData.getLength();
        //建立存储tuple的数组
        List<Tuple> temp = new ArrayList<>();
        for(int i = 0; i < tupleCount; i++){
            //重新从page读取tuple
            ItemPointer ptr = new ItemPointer(page.readInt(), page.readInt());
            if(ptr.getTupleLength() == -1){
                continue;
            }
            byte[] bb = page.readBytes(ptr.getOffset(), ptr.getTupleLength());
            Tuple tuple = new Tuple();
            tuple.read(bb);
            temp.add(tuple);
            //下一个元组位置
            ptrStartOff += ptr.getTupleLength();
        }
        //可能被删除，置为-1,所以以temp的内容为准
        tuples = temp.toArray(new Tuple[temp.size()]);
        tupleCount = temp.size();
    }

    public Tuple[] getTuples() {
        return tuples;
    }
    public int getTupleCount() {
        return tupleCount;
    }
}
