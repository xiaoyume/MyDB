package org.mydb.meta.value;

import org.junit.Test;

import static org.junit.Assert.*;

public class ValueBooleanTest {

    @Test
    public void getLength() {
        assertEquals(2, new ValueBoolean(true).getLength());
    }

    @Test
    public void getType() {
        assertEquals(ValueBoolean.BOOLEAN, new ValueBoolean(true).getType());
    }

    @Test
    public void getBytes() {
        assertArrayEquals(new byte[]{4, 1}, new ValueBoolean(true).getBytes());
    }

    @Test
    public void read() {
        ValueBoolean v = new ValueBoolean();
        v.read(new byte[]{4, 1});
        assertEquals(v.getBoolean(), true);
    }

}