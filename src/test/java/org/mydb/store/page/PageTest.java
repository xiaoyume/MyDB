package org.mydb.store.page;

import org.junit.Test;
import org.mydb.config.SystemConfig;
import org.mydb.meta.Tuple;
import org.mydb.meta.value.*;
import org.mydb.store.fs.FStore;
import org.mydb.store.item.Item;

import static org.junit.Assert.*;

public class PageTest {

    @Test
    public void pageTest1(){
        Value[] values = new Value[5];
        values[0] = new ValueString("this is freedom db");
        System.out.println(values[0].getLength());
        values[1] = new ValueString("just do it");
        values[2] = new ValueBoolean(true);
        values[3] = new ValueInt(5);
        values[4] = new ValueLong(6l);
        Tuple tuple = new Tuple(values);
        System.out.println(tuple.getLength());
        Item item = new Item(tuple);
        System.out.println(item.getLength());
        PagePool pagePool = PagePool.getInstance();
        Page page = pagePool.getFreePage();
        for(int i = 0; i < 2; i++){
            if(page.writeItem(item)){
                continue;
            }else{
                System.out.println("btee=" + i + ", page size exhaust");
                break;
            }
        }
        FStore fStore = new FStore(SystemConfig.FREEDOM_REL_PATH);
        fStore.open();
        fStore.writePageToFile(page, 0);
        fStore.writePageToFile(page, 10);

        PageLoader loader = fStore.readPageLoaderFromFile(10);
        Tuple[] tuples = loader.getTuples();
        for(Tuple t : tuples){
            for(Value v : t.getValues()){
                System.out.println(v);
            }
        }
    }
    @Test
    public void test2(){
        Value[] values = new Value[1];
        values[0] = new ValueString("11111");
        Tuple tuple = new Tuple(values);
        System.out.println(tuple.getLength());
        Item item = new Item(tuple);
        System.out.println(item.getLength());
        PagePool pagePool = PagePool.getInstance();
        Page freePage = pagePool.getFreePage();
        freePage.writeItem(item);
        FStore fStore = new FStore(SystemConfig.FREEDOM_REL_PATH);
        fStore.open();
        fStore.writePageToFile(freePage, 0);
        PageLoader loader = fStore.readPageLoaderFromFile(0);
        Tuple[] tuples = loader.getTuples();
        Value[] values1 = tuples[0].getValues();
        System.out.println(values1[0]);
    }

}