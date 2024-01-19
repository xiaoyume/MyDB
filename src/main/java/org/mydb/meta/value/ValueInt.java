package org.mydb.meta.value;

import org.mydb.utils.BufferWrapper;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2023/11/28 10:48
 */
public class ValueInt extends Value{

    private int i;

    public ValueInt() {
    }

    public ValueInt(int i) {
        this.i = i;
    }
    @Override
    public int getLength() {
        return 1 + 4;
    }

    @Override
    public byte getType() {
        return INT;
    }

    @Override
    public byte[] getBytes() {
        BufferWrapper bufferWrapper = new BufferWrapper(getLength());
        bufferWrapper.writeByte(INT);
        bufferWrapper.writeInt(i);
        return bufferWrapper.getBuffer();
    }
    /**
     * 输入字节数组，读出数据
     * @param bytes
     */
    @Override
    public void read(byte[] bytes) {
        BufferWrapper wrapper = new BufferWrapper(bytes);
        i = wrapper.readInt();
    }

    @Override
    public String toString() {
        return String.valueOf(i);
    }

    public int getInt() {
        return i;
    }

    public ValueInt setI(int i) {
        this.i = i;
        return this;
    }

    /**
     * 等 0 大于 1 小于 -1
     * @param value
     * @return
     */
    @Override
    public int compare(Value value) {
        int anInt = ((ValueInt) value).getInt();
        if(i > anInt){
            return 1;
        }else if(i < anInt){
            return -1;
        }else {
            return 0;
        }
    }
}
