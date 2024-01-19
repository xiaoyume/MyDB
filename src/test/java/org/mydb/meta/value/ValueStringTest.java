package org.mydb.meta.value;

import org.junit.Test;

import static org.junit.Assert.*;

public class ValueStringTest {
    @Test
    public void test(){
        byte[] b = "test".getBytes();
        ValueString valueString = new ValueString();
        System.out.println(b.length);
        valueString.read(b);
        assertEquals(valueString.getString(), "test");
        assertEquals(valueString.getLength(), 9);
        assertEquals(1, valueString.getType());
    }

}