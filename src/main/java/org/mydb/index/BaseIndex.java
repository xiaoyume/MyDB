package org.mydb.index;

import org.mydb.config.SystemConfig;
import org.mydb.meta.Attribute;
import org.mydb.meta.Relation;
import org.mydb.meta.Tuple;
import org.mydb.meta.value.Value;
import org.mydb.store.fs.FStore;
import org.mydb.store.page.PageNoAllocator;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2023/12/25 19:51
 */
public abstract class BaseIndex implements Index{

    //先假定当前key是唯一的，先处理key唯一的情况


    //哪个表
    protected Relation relation;
    //索引名称
    protected String indexName;
    //索引用到的属性项
    protected Attribute[] attributes;
    //索引所在的文件具体位置
    protected String path;
    protected FStore fStore;
    protected boolean isUnique;
    protected PageNoAllocator pageNoAllocator;
    protected boolean isPrimaryKey;

    public BaseIndex(){

    }

    public BaseIndex(Relation relation, String indexName, Attribute[] attributes){
        this.relation = relation;
        this.indexName = indexName;
        this.relation = relation;
        path = SystemConfig.RELATION_FILE_PRE_FIX + indexName;
        pageNoAllocator = new PageNoAllocator();
        fStore = new FStore(path);
        fStore.open();
        isUnique = false;
        isPrimaryKey = true;
    }

    /**
     * 获取下一个pageNo
     * @return
     */
    public int getNextPageNo(){
        return pageNoAllocator.getNextPageNo();
    }

    public void recyclePageNo(int pageNo){
        pageNoAllocator.recycleCount(pageNo);
    }

    public abstract void flushToDisk();

    /**
     * 从tuple中组织出属性列对应的索引key
     * 因为一个tuple包含多个列，key的列可能不和tuple的列一一对应，所以需要转换
     * @param tuple
     * @return
     */
    public Tuple convertToKey(Tuple tuple){
        Value[] values = new Value[attributes.length];
        for(int i = 0; i < values.length; i++){
            Attribute attribute = attributes[i];
            //
            values[i] = tuple.getValues()[attribute.getIndex()];
        }
        return new Tuple(values);
    }

    public boolean isUnique(){
        return isUnique;
    }
    public BaseIndex setUnique(boolean unique){
        isUnique = unique;
        return this;
    }
    public boolean isPrimaryKey(){
        return isPrimaryKey;
    }
    public BaseIndex setPrimaryKey(boolean primaryKey){
        isPrimaryKey = primaryKey;
        return this;
    }
}

