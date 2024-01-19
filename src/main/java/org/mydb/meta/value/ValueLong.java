package org.mydb.meta.value;

import org.mydb.utils.BufferWrapper;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2023/11/28 10:58
 */
public class ValueLong extends Value{

    private long l;
    public ValueLong(long l) {
        this.l = l;
    }

    public ValueLong() {
    }

    @Override
    public int getLength() {
        return 1 + 8;
    }

    @Override
    public byte getType() {
        return LONG;
    }

    @Override
    public byte[] getBytes() {
        BufferWrapper wrapper = new BufferWrapper(9);
        wrapper.writeByte(LONG);
        wrapper.writeLong(l);
        return wrapper.getBuffer();
    }

    @Override
    public void read(byte[] bytes) {
        BufferWrapper wrapper = new BufferWrapper(bytes);
        l = wrapper.readLong();
    }

    @Override
    public String toString() {
        return String.valueOf(l);
    }

    public long getLong() {
        return l;
    }

    public ValueLong setLong(long l) {
        this.l = l;
        return this;
    }

    /**
     * 等0 大于1 小于-1
     * @param value
     * @return
     */
    @Override
    public int compare(Value value) {
        long aLong = ((ValueLong) value).getLong();
        if(l > aLong){
            return 1;
        }else if(l < aLong){
            return -1;
        }else{
            return 0;
        }
    }
}
