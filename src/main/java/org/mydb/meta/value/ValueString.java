package org.mydb.meta.value;

import org.mydb.utils.BufferWrapper;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2023/11/28 12:13
 */
public class ValueString extends Value{
    //实际存储的数据
    private String s;
    public ValueString(String s){
        this.s = s;
    }

    public ValueString() {
    }

    /**
     * [type][length][data]
     * @return
     */
    @Override
    public int getLength() {
        return 1 + 4 + s.length();
    }

    @Override
    public byte getType() {
        return STRING;
    }

    @Override
    public byte[] getBytes() {
        BufferWrapper wrapper = new BufferWrapper(getLength());
        wrapper.writeByte(STRING);//写类型
        wrapper.writeStringLength(s);//写长度
        wrapper.writeBytes(s.getBytes());//写数据
        return wrapper.getBuffer();
    }

    @Override
    public void read(byte[] bytes) {
        s = new String(bytes);
    }

    @Override
    public String toString() {
        return s;
    }

    public String getString() {
        return s;
    }

    public ValueString setString(String s) {
        this.s = s;
        return this;
    }

    @Override
    public int compare(Value value) {
        return s.compareTo(((ValueString)value).getString());
    }
}
