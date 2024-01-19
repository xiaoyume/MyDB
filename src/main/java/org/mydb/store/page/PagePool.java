package org.mydb.store.page;

import java.util.AbstractQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2023/12/12 19:04
 */
public class PagePool {
    private static PagePool pagePool;
    private static int defaultPageNum = 8;
    private AbstractQueue<Page> frees = new ConcurrentLinkedQueue<>();
    private PageFactory factory = PageFactory.getInstance();
    static {
        pagePool = new PagePool();
        pagePool.init();
    }

    private void init() {
        //初始化8页数据
        for(int i = 0; i < defaultPageNum; i++){
            frees.add(factory.newPage());
        }
    }
    public static PagePool getInstance(){
        return pagePool;
    }
    public Page getFreePage(){
        return factory.newPage();
    }
    public void recycle(Page page){
        page.clean();
        frees.add(page);
    }
}
