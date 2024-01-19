package org.mydb.store.page;

import org.mydb.meta.Tuple;
import org.mydb.store.item.Item;
import org.mydb.utils.BufferWrapper;

import java.util.List;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2023/12/8 16:33
 */
public class Page {
    protected PageHeaderData pageHeaderData;
    protected BufferWrapper bufferWrapper;

    protected  int length;
    //是否是脏页
    protected boolean dirty;
    public Page(int defaultSize){
        pageHeaderData = new PageHeaderData(defaultSize);
        //bufferwrapper是写bytebuffer的
        bufferWrapper = new BufferWrapper(new byte[defaultSize]);
        length = defaultSize;
        pageHeaderData.write(this);
        dirty = false;
    }

    public void read(byte[] buffer){
        bufferWrapper = new BufferWrapper(buffer);
    }

    public void addTupleCount(Page page) {
        pageHeaderData.addTupleCount(page);
    }
    public void decTupleCount(Page page){
        pageHeaderData.decTupleCount(page);
    }

    /**
     * 判断页内free空间是否足够
     * @param tuple
     * @return
     */
    public boolean spaceEnough(Tuple tuple){
        Item item = new Item(tuple);
        if(remainFreeSpace() < item.getLength()){
            return false;
        }else {
            return true;
        }
    }
    
    public boolean writeTuple(Tuple tuple){
        return writeItem(new Item(tuple));
    }

    /**
     * 写item项，写入失败返回false，写入成功返回true
     * @param item
     * @return
     */
    public boolean writeItem(Item item) {
        if(item.writeItem(this)){
            //表示此页已脏
            dirty = true;
            return true;
        }else{
            return false;
        }
    }
    public void writeItems(List<Item> items){
        for(Item item : items){
            if(this.writeItem(item)){
                continue;
            }else{
                throw new RuntimeException("Meta Info too long");
            }
        }
    }

    public void WriteByte(byte b){
        bufferWrapper.writeByte(b);
    }
    public void writeLong(long l) {
        bufferWrapper.writeLong(l);
    }


    public void writeIntPos(int i, int pos) {
        bufferWrapper.writeIntPos(i, pos);
    }

    public int readIntPos(int pos) {
        return bufferWrapper.readIntPos(pos);
    }

    public void writeStringWithNull(String s) {
        bufferWrapper.writeStringWithNull(s);
    }

    public void writeInt(int i) {
        bufferWrapper.writeInt(i);
    }

    public int getLength() {
        return length;
    }

    public String readStringWithNull() {
        return bufferWrapper.readStringWithNull();
    }

    public int readInt() {
        return bufferWrapper.readInt();
    }
    public int getLowerOffset() {
        return pageHeaderData.getLowerOffset();
    }

    public void modifyLowerOffset(int i) {
        pageHeaderData.modifyLowerOffset(i, this);
    }

    public int geUpperOffset() {
        return pageHeaderData.getUpperOffset();
    }

    public void modifyUpperOffset(int i) {
        pageHeaderData.modifyUpperOffset(i, this);
    }

    public void writeBytes(byte[] bytes, int writePos) {
        bufferWrapper.writeBytes(bytes, writePos);
    }

    public byte[] readBytes(int pos, int length) {
        return bufferWrapper.readBytes(pos, length);
    }

    public int remainFreeSpace() {
        return pageHeaderData.getUpperOffset() - pageHeaderData.getLowerOffset();
    }

    public void clean() {
        bufferWrapper.clean();
        dirty = false;
        pageHeaderData = new PageHeaderData(bufferWrapper.getLength());
        pageHeaderData.write(this);
    }
    public byte[] getBuffer() {
        return bufferWrapper.getBuffer();
    }

    public boolean isDirty() {
        return dirty;
    }

    public Page setDirty(boolean dirty) {
        this.dirty = dirty;
        return this;
    }
}
