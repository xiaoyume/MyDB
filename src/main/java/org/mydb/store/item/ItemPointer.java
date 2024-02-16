package org.mydb.store.item;

import org.mydb.store.page.Page;

/**
 * @author xiaoy
 * @version 1.0
 * @description: tuple的指针，指向tuple的起始位置和长度
 * @date 2023/12/8 19:30
 */
public class ItemPointer {
    private int offset;
    private int tupleLength;
    public ItemPointer(int offset, int tupleLength) {
        this.offset = offset;
        this.tupleLength = tupleLength;
    }

    /**
     * 写入一个指针，指向tuple的offset，和长度
     * @param page
     */
    void write(Page page){
        page.writeInt(offset);
        page.writeInt(tupleLength);
        int lowerOffset = page.getLowerOffset();
        lowerOffset += getPtrLength();
        page.modifyLowerOffset(lowerOffset);
    }

    public static int getPtrLength(){
        return 8;
    }
    public int getTupleLength(){
        return tupleLength;
    }
    public int getOffset(){
        return offset;
    }
    public ItemPointer setOffset(int offset){
        this.offset = offset;
        return this;
    }
}
