package org.mydb.store.page;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2023/12/12 19:23
 */
public class PageNoAllocator {
    private AtomicInteger count;
    private List<Integer> freePageNoList;
    public PageNoAllocator(){
        count = new AtomicInteger(0);
        freePageNoList = new LinkedList<>();
    }

    /**
     * 获取下一个页号，如果没有空闲页号，则从count中获取下一个页号
     * @return
     */
    public int getNextPageNo(){
        if(freePageNoList.size() == 0){
            return count.getAndAdd(1);
        }
        //从空闲页号中获取下一个页号
        return freePageNoList.remove(0);
    }

    /**
     * 回收页号到空闲页号列表中
     * @param pageNo
     */
    public void recycleCount(int pageNo){
        freePageNoList.add(pageNo);
    }
    //从磁盘中重新构造page时候，需要重新设置其pageNo
    public void setCount(int lastPageNo){
        count.set(lastPageNo + 1);
    }
}
