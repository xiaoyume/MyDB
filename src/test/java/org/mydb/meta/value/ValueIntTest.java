package org.mydb.meta.value;

import org.junit.Test;

import static org.junit.Assert.*;

public class ValueIntTest {

    @Test
    public void getLength() {
        ValueInt valueInt = new ValueInt();
        byte[] b = new byte[]{1, 0, 0, 0};
        valueInt.read(b);
        assertEquals(1, valueInt.getInt());
        assertEquals(5, valueInt.getLength());
        assertEquals(2, valueInt.getType());
        assertEquals(2, valueInt.getBytes()[0]);
    }
}