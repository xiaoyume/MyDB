package org.mydb.access;

import org.mydb.meta.Tuple;

/**
 * 扫描器
 */
public interface Cursor {
    Tuple getNext();
}
