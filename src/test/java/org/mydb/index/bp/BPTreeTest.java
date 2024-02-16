package org.mydb.index.bp;

import org.junit.Test;
import org.mydb.meta.Tuple;
import org.mydb.meta.value.Value;
import org.mydb.meta.value.ValueInt;
import org.mydb.meta.value.ValueString;

import java.util.Random;

import static org.junit.Assert.*;

public class BPTreeTest {

    @Test
    public void testInsert(){
        BPTree bpTree = new BPTree(null, "bpindex", null);
        int insertSize = 150;
        for(int i = 0; i <= insertSize; i++){
            Value[] values = new Value[2];
            values[0] = new ValueInt(i);
            values[1] = new ValueString("xiaoyumeDB");
            Tuple tuple = new Tuple(values);
            bpTree.insert(tuple, true);
//            printBtree(bpTree.getRoot());
        }
        printBtree(bpTree.getRoot());
    }

    @Test
    public void test2(){
        BPTree bpTree = new BPTree(null, "bptest", null);
        Tuple t1 = genTuple(3);
        Tuple t2 = genTuple(2);
        Tuple t3 = genTuple(1);
        Tuple t4 = genTuple(4);
        Tuple t5 = genTuple(5);
        bpTree.insert(t1, true);
        bpTree.insert(t2, true);
        bpTree.insert(t3, true);
        bpTree.insert(t4, true);
        bpTree.insert(t5, true);
        pBtree(bpTree);
        bpTree.remove(t1);
        System.out.println("*********************8\n");
        pBtree(bpTree);


    }


    public void printBtree(BPNode bpNode) {
        if (bpNode == null) {
            return;
        }

        if ((!bpNode.isLeaf()) && ((bpNode.getEntries().size() + 1) != bpNode.getChildren().size())) {
            System.out.println("B+Tree Error");
        }

        double spaceRate = bpNode.getBpPage().getContentSize() * 1.0 / bpNode.getBpPage().getInitFreeSpace();
        System.out.println("node space rate=" + spaceRate);

        if (!bpNode.isLeaf()) {
            for (int i = 0; i < bpNode.getChildren().size(); i++) {
                //验证父节点是否正确
                if (bpNode.getChildren().get(i).getParent() != bpNode) {
                    System.out.println("parent BPNode error");
                    throw new RuntimeException("error");
                }
                //验证子节点和entry数目是否能对上，父节点entry+1等于子节点数目
                if (bpNode.getEntries().size() + 1 != bpNode.getChildren().size()) {
                    throw new RuntimeException("cacaca error");
                }
                //
                if (i < bpNode.getEntries().size()) {
                    //验证节点的第i个entry值是否比第i个子节点的最后一个entry大 10 > 9
                    //         10
                    //    9         11
                    if (bpNode.getEntries().get(i)
                            .compare(bpNode.getChildren().get(i).getEntries().get(bpNode.getChildren
                                    ().get(i).getEntries().size() - 1)) <= 0) {
                        throw new RuntimeException("hahaha error");
                    }
                }
                if (i == bpNode.getEntries().size()) {
                    if (bpNode.getEntries().get(i - 1)
                            .compare(bpNode.getChildren().get(i).getEntries().get(bpNode.getChildren
                                    ().get(i).getEntries().size() - 1)) > 0) {
                        throw new RuntimeException("hahaha error");
                    }
                }
                printBtree(bpNode.getChildren().get(i));
            }
        }

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
        if (str.length() > 80) {
            str = str.substring(0, 80);
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