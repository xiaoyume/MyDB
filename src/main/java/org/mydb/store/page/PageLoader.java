package org.mydb.store.page;

import org.mydb.meta.Tuple;
import org.mydb.store.item.ItemPointer;
import org.mydb.store.page.Page;
import org.mydb.store.page.PageHeaderData;

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
        int ptrStartOff = pageHeaderData.geLength();
        tuples = new Tuple[tupleCount];
        for(int i = 0; i < tupleCount; i++){
            //重新从page读取tuple
            ItemPointer ptr = new ItemPointer(page.readInt(), page.readInt());
            byte[] bb = page.readBytes(ptr.getOffset(), ptr.getTupleLength());
            Tuple tuple = new Tuple();
            tuple.read(bb);
            tuples[i] = tuple;
            //下一个元组位置
            ptrStartOff += ptr.getTupleLength();
        }
    }

    public Tuple[] getTuples() {
        return tuples;
    }
    public int getTupleCount() {
        return tupleCount;
    }
}
