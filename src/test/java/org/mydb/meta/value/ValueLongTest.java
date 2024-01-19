package org.mydb.meta.value;

import org.junit.Test;

import static org.junit.Assert.*;

public class ValueLongTest {
    @Test
    public void test() {
        ValueLong valueLong = new ValueLong();
        byte[] b = new byte[]{1, 0, 0, 0, 0, 0, 0, 0};
        valueLong.read(b);
        assertEquals(1, valueLong.getLong());
        assertEquals(9, valueLong.getLength());
        assertEquals(3, valueLong.getType());
    }

}