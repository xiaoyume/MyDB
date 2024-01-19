package org.mydb.meta;

import org.mydb.meta.value.Value;
import org.mydb.store.item.Item;
import org.mydb.utils.ValueConverUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 元组属性描述
 * @date 2023/11/30 19:39
 */
public class TupleDesc {
    private Attribute[] attrs;
    private Map<String, Attribute> attrsMap;

    /**
     * 元组属性描述
     * 把属性数组转化为属性map，键为属性中的name
     * @param attrs
     */
    public TupleDesc(Attribute[] attrs) {
        this.attrs = attrs;
        attrsMap = new HashMap<String, Attribute>();
        for(Attribute attr : attrs){
            attrsMap.put(attr.getName(), attr);
        }
    }

    /**
     * attributes转换为可写入page的item
     * @return
     */
    public List<Item> getItems(){
        List<Item> list = new ArrayList<Item>();
        for(Attribute attribute : attrs){
            Value[] values = ValueConverUtil.convertAttr(attribute);
            Tuple tuple = new Tuple(values);
            list.add(new Item(tuple));
        }
        return list;
    }




}
