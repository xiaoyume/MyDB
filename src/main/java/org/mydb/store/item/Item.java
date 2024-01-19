package org.mydb.store.item;

import org.mydb.meta.Tuple;
import org.mydb.store.page.Page;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2023/11/30 20:53
 */
public class Item {
    //两个指针：偏移量和长度
    private ItemPointer ptr;
    private ItemData data;

    public Item(Tuple tuple){
        data = new ItemData(tuple);
        ptr = new ItemPointer(0, data.getLength());
    }
    public boolean writeItem(Page page){
        int freeSpace = page.remainFreeSpace();
        if(freeSpace < getLength()){
            return false;
        }
        //指针从低到高，数据从高到低
        data.write(page);
        ptr.setOffset(data.getOffset());
        ptr.write(page);
        page.addTupleCount(page);
        return true;
    }
    public int getLength(){
        return data.getLength() + ptr.getPtrLength();
    }
    public static int getItemLength(Tuple key){
        return key.getLength() + 8;
    }
}
