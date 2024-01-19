package org.mydb.access;

import org.mydb.meta.Tuple;

/**
 * 扫描器
 */
public interface Scanner {
    Tuple getNext();
}
