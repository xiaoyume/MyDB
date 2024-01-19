package org.mydb.store.item;

import org.mydb.meta.Tuple;
import org.mydb.store.page.Page;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2023/11/30 20:53
 */
public class ItemData {
    //帧结构
    //[length]([type][length][data]])*
    private Tuple tuple;
    private int offset;
    //Item实际长度
    private int length;

    public ItemData(Tuple tuple){
        this.tuple = tuple;
        length = tuple.getLength();
    }

    /**
     * upperoffset
     * @param page
     */
    public void write(Page page){
        //获取总长度
        int tupleLength = length;
        //找到写入位置, 从writepos位置写入，并修改upperoffset到writeoffset位置，从后往前
        int writePos = page.geUpperOffset() - tupleLength;
        page.writeBytes(tuple.getBytes(), writePos);
        page.modifyUpperOffset(writePos);
        offset = writePos;
    }

    public int getOffset(){
        return offset;
    }
    public ItemData setOffset(int offset){
        this.offset = offset;
        return this;
    }
    public int getLength(){
        return length;
    }
    public ItemData setLength(int length){
        this.length = length;
        return this;
    }

}
