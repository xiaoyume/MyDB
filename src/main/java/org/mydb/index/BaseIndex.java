package org.mydb.index;

import org.mydb.config.SystemConfig;
import org.mydb.meta.Attribute;
import org.mydb.meta.Relation;
import org.mydb.meta.Tuple;
import org.mydb.store.fs.FStore;
import org.mydb.store.page.PageNoAllocator;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2023/12/25 19:51
 */
public abstract class BaseIndex implements Index{
    //哪个表
    protected Relation relation;
    //索引名称
    protected String indexName;
    //索引用到的属性项
    protected Attribute[] attributes;
    //索引所在的文件具体位置
    protected String path;
    protected FStore fStore;
    protected PageNoAllocator pageNoAllocator;

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
}
