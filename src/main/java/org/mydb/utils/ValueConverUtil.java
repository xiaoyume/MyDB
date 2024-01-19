package org.mydb.utils;

import org.mydb.meta.Attribute;
import org.mydb.meta.value.Value;
import org.mydb.meta.value.ValueInt;
import org.mydb.meta.value.ValueString;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2023/11/30 20:22
 */
public class ValueConverUtil {
    /**
     * 把Attribute转换为Value数组
     * @param attr
     * @return
     */
    public static Value[] convertAttr(Attribute attr){
        List<Value> list = new ArrayList<>();
        list.add(new ValueString(attr.getName()));
        list.add(new ValueInt(attr.getType()));
        list.add(new ValueInt(attr.getIndex()));
        list.add(new ValueString(attr.getComment()));
        return list.toArray(new Value[list.size()]);
    }

    public static Attribute convertValue(Value[] values){
        Attribute attribute = new Attribute();
        attribute.setName(((ValueString)values[0]).getString());
        attribute.setType(((ValueInt)values[1]).getInt());
        attribute.setIndex(((ValueInt)values[2]).getInt());
        attribute.setComment(((ValueString)values[3]).getString());
        return attribute;
    }

}
