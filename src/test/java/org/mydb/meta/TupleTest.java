package org.mydb.meta;

import org.junit.Test;
import org.mydb.meta.value.*;
import org.mydb.utils.BufferWrapper;

import static org.junit.Assert.*;

public class TupleTest {
    @Test
    public void test(){
        ValueBoolean valueBoolean = new ValueBoolean(true);
        ValueInt valueInt = new ValueInt(123);
        ValueLong valueLong = new ValueLong(12344l);
        ValueString valueString = new ValueString("ssssss");
        Value[] v = new Value[]{valueBoolean, valueInt, valueLong, valueString};
        Tuple tuple = new Tuple(v);
        byte[] bytes = tuple.getBytes();
        BufferWrapper wrapper = new BufferWrapper(bytes);
        //验证boolean类型
        byte b = wrapper.readByte();
        assertEquals(4, b);
        byte bool = wrapper.readByte();
        assertEquals(1, bool);
        //验证int类型
        byte in = wrapper.readByte();//2
        assertEquals(2, in);
        int i = wrapper.readInt();
        assertEquals(123, i);
        //验证long类型
        byte lo = wrapper.readByte();//3
        assertEquals(3, lo);
        long l = wrapper.readLong();
        assertEquals(12344l, l);
        //验证string类型
        byte st = wrapper.readByte();//4
        assertEquals(1, st);
        int ii = wrapper.readInt();
        assertEquals(6, ii);
        String s = wrapper.readStringWithNull();
        assertEquals("ssssss", s);
        int length = tuple.getLength();
        assertEquals(27, length);
        Value[] vi = new Value[]{valueBoolean, valueInt, valueLong};
        Tuple tuple1 = new Tuple(vi);
        System.out.println(tuple.compare(tuple1));
    }

}