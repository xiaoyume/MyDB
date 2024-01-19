package org.mydb.store.page;

import org.mydb.config.SystemConfig;
import org.mydb.index.bp.BPNode;
import org.mydb.index.bp.BPPage;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2023/12/11 20:37
 */
public class PageFactory {
    private static PageFactory factory = new PageFactory();
    public static PageFactory getInstance() {
        return factory;
    }
    private PageFactory(){}
    public Page newPage(){
        return new Page(SystemConfig.DEFAULT_PAGE_SIZE);
    }

    public BPPage newBPPage(BPNode bpNode){
        return new BPPage(bpNode);
    }

}
