package org.mydb.access;

import org.mydb.meta.Relation;
import org.mydb.meta.Tuple;
import org.mydb.store.page.Page;
import org.mydb.store.page.PageLoader;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 顺序扫描关系表中的元组
 * @date 2023/12/28 15:43
 */
public class SeqScanner implements Scanner{

    private Relation relation;
    //当前page,从第一页开始，第0页为pageoffset页
    private int currentPageNo = 1;
    //当前页的元组计数
    private int currentTupleCount = 0;
    private PageLoader currentPageLoader;

    public SeqScanner(Relation relation) {
        this.relation = relation;
    }
    @Override
    public Tuple getNext() {
        if(currentPageLoader == null){
            loadPage(currentPageNo);
        }
        //当前页还有元组
        if(currentTupleCount < currentPageLoader.getTupleCount()){
            return currentPageLoader.getTuples()[currentTupleCount++];
        }else{
            //这页没元组，到新页
            currentTupleCount = 0;
            currentPageNo ++;
            if(currentPageNo > relation.getPageCount()){
                //超过了关系表的页数，返回null
                return null;
            }
            loadPage(currentPageNo);
            return currentPageLoader.getTuples()[currentTupleCount++];
        }
    }

    private void loadPage(int pageNo){
        Page page = relation.getPageMap().get(currentPageNo);
        currentPageLoader = new PageLoader(page);
        currentPageLoader.load();
    }
}
