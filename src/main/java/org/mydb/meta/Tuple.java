package org.mydb.meta;

import org.mydb.meta.value.*;
import org.mydb.utils.BufferWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 存储值的元组
 * @date 2023/11/29 15:26
 */
public class Tuple {
    protected Value[] values;

    public Tuple(Value[] values) {
        this.values = values;
    }
    public Tuple(){}

    /**
     * 元组序列化
     * @return
     */
    public byte[] getBytes() {
        byte[] bb = new byte[getLength()];
        int pos = 0;
        for(Value item : values){
            System.arraycopy(item.getBytes(), 0, bb, pos, item.getLength());
            pos += item.getLength();
        }
        return bb;
    }

    /**
     * 从字节数组中读取元组
     * @param bytes
     */
    public void read(byte[] bytes){
        BufferWrapper wrapper = new BufferWrapper(bytes);
        List<Value> res = new ArrayList<>();
        while(wrapper.remaining() > 0){
            int type = wrapper.readByte();//值的类型
            byte[] bs = null;
            Value value = null;
            switch (type){
                case Value.INT -> {
                    bs = wrapper.readBytes(4);
                    value = new ValueInt();
                    break;
                }
                case Value.BOOLEAN -> {
                    bs = wrapper.readBytes(1);
                    value = new ValueBoolean();
                    break;
                }
                case Value.LONG -> {
                    bs = wrapper.readBytes(8);
                    value = new ValueLong();
                    break;
                }
                case Value.STRING -> {
                    int length = wrapper.readInt();//获取长度
                    bs = wrapper.readBytes(length);
                    value = new ValueString();
                    break;
                }
                default -> {
                    throw new RuntimeException("no support type");
                }
            }
            value.read(bs);
            res.add(value);
        }
        values = res.toArray(new Value[res.size()]);
    }

    /**
     * 另一个tuple可能是一个索引，所以两者的column的length不等
     * 同时由于最终索引会加两个值表示pageNo和offset，所以应该比传进来的tuple长度大
     * @param tuple
     * @return
     */
    public int compareIndex(Tuple tuple){
        return compare(tuple);
    }

    /**
     * 比较两个元组是否相等
     * @param tuple
     * @return 比较逻辑是，
     */
    public int compare(Tuple tuple){
        int min = values.length < tuple.getValues().length ? values.length : tuple.getValues().length;
        int comp = 0;
        for(int i = 0; i < min; i++){
           comp = values[i].compare(tuple.getValues()[i]);
           if(comp== 0){
                continue;
           }
           return comp;
        }
        //for循环表示前面的都相等
        if(comp == 0){
            //tuple每一项都相同，长度也相同
            if(values.length == tuple.getValues().length){
                return 0;
            }
            //本长度小
            if(values.length < tuple.getValues().length){
                return -1;
            }else{
                return 1;
            }
        }
        return comp;
    }

    /**
     * 计算元组使用的字节长度
     * @return
     */
    public int getLength(){
        int sum = 0;
        for(Value item : values){
            sum += item.getLength();
        }
        return sum;
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "values=" + Arrays.toString(values) +
                '}';
    }
    public Value[] getValues() {
        return values;
    }
    public Tuple setValues(Value[] values) {
        this.values = values;
        return this;
    }
}
