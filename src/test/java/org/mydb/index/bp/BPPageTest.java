package org.mydb.index.bp;

import org.junit.Test;
import org.mydb.meta.Tuple;
import org.mydb.meta.value.Value;
import org.mydb.meta.value.ValueInt;
import org.mydb.meta.value.ValueString;

import java.util.Random;

import static org.junit.Assert.*;

public class BPPageTest {
    @Test
    public void testWrite(){
        BPTree bpTree = new BPTree(null, "bpIndex", null);
        int insertSize = 10;
        for(int i = 0; i < insertSize; i++){
            Random random = new Random();
            int i1 = random.nextInt();
            Tuple tuple = genTuple(i1);
            bpTree.insert(tuple);
        }
        bpTree.flushToDisk();
    }

    @Test
    public void testRead(){
        BPTree bpTree = new BPTree(null, "bpIndex", null);
        bpTree.loadFromDisk();
        pBtree(bpTree);
    }

    public static Tuple genTuple(int i) {
        Value[] values = new Value[2];
        values[0] = new ValueInt(i);
        Random random = new Random();
        int strSize = random.nextInt(20) + 1;
        String str = "";
        for (int j = 0; j < strSize; j++) {
            str = str + random.nextInt();
        }
        if (str.length() > 40) {
            str = str.substring(0, 40);
        }
        values[1] = new ValueString(str);
        return new Tuple(values);
    }
    public void pBtree(BPTree bpTree){
        if(bpTree.getHead()==null){
            return;
        }
        BPNode tmp = bpTree.getHead();
        int count = 1;
        while(tmp != null){
            for(int i = 0; i < tmp.getEntries().size(); i++){
                count ++;
                System.out.println(tmp.getEntries().get(i));
            }
            tmp = tmp.getNext();
        }
    }

}