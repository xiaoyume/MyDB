package org.mydb.index.bp;

import org.mydb.config.SystemConfig;
import org.mydb.constant.ItemConst;
import org.mydb.meta.Tuple;
import org.mydb.meta.value.Value;
import org.mydb.meta.value.ValueInt;
import org.mydb.store.item.Item;
import org.mydb.store.page.Page;
import org.mydb.store.page.PageHeaderData;
import org.mydb.store.page.PageLoader;

/**
 * @author xiaoy
 * @version 1.0
 * @description: b+树 页
 * @date 2023/12/15 16:00
 */
public class BPPage extends Page {
    private BPNode bpNode;

    public BPPage(int defaultSize) {
        super(defaultSize);
    }

    public BPPage(BPNode bpNode) {
        super(SystemConfig.DEFAULT_PAGE_SIZE);
        this.bpNode = bpNode;
    }

    /**
     * 从磁盘读取节点
     *
     * @param bpTree
     * @return
     */
    public BPNode readFromPage(BPTree bpTree) {
        PageLoader loader = null;
        try {
            loader = new PageLoader(this);
            loader.load();

            boolean isLeaf = getTupleBoolean(loader.getTuples()[0]);
            boolean isRoot = getTupleBoolean(loader.getTuples()[1]);

            bpNode = new BPNode(isLeaf, isRoot, bpTree);
            //从磁盘读取，一磁盘记录为准
            int pageNo = getTupleInt(loader.getTuples()[2]);
            bpNode.setPageNo(pageNo);
            //先放入nodeMap，否则由于一直递归，一直没机会放入，导致
            bpTree.nodeMap.put(pageNo, bpNode);
            int parentPageNo = getTupleInt(loader.getTuples()[3]);
            bpNode.setParent(bpTree.getNodeFromPageNo(pageNo));
            int entryCount = getTupleInt(loader.getTuples()[4]);
            for (int i = 0; i < entryCount; i++) {
                bpNode.getEntries().add(loader.getTuples()[5 + i]);
            }
            if (!isLeaf) {
                int childCount = getTupleInt(loader.getTuples()[5 + entryCount]);
                int initSize = 6 + entryCount;
                for (int i = 0; i < childCount; i++) {
                    int childPageNo = getTupleInt(loader.getTuples()[initSize + i]);
                    bpNode.getChildren().add(bpTree.getNodeFromPageNo(childPageNo));
                }
            } else {
                int initSize = 5 + entryCount;
                int previousNo = getTupleInt(loader.getTuples()[initSize]);
                int nextNo = getTupleInt(loader.getTuples()[initSize + 1]);
                bpNode.setPrevious(bpTree.getNodeFromPageNo(previousNo));
                bpNode.setNext(bpTree.getNodeFromPageNo(nextNo));
            }
            return bpNode;
        } catch (Exception e) {
            System.out.println("bug need fix!!!!!!!!!!");
            System.out.println(this.getInitFreeSpace());
            throw new RuntimeException("bug!!!!!");
        }
    }

    /**
     * 把page写到文件里
     */
    public void writeToPage() {
        //第一个tuple 表示是否是叶子节点 1 0
        writeTuple(genIsLeafTuple());
        //第二个tuple 表示是否是根节点 1 0
        writeTuple(genIsRootTuple());
        //第三个tuple 表示页号
        writeTuple(genTupleInt(bpNode.getPageNo()));
        //写父节点页号
        if (!bpNode.isRoot) {
            writeTuple(genTupleInt(bpNode.getParent().getPageNo()));
        } else {
            //表示是root页面
            writeTuple(genTupleInt(-1));
        }
        //写关键字链 数量
        writeTuple(genTupleInt(bpNode.getEntries().size()));
        //entries
        for (int i = 0; i < bpNode.getEntries().size(); i++) {
            if (!writeTuple(bpNode.getEntries().get(i))) {
                System.out.println("size err!!");
                int size = this.getContentSize();
                System.out.println("content size= " + size + ", init free space= " + this.getInitFreeSpace());
                writeTuple(bpNode.getEntries().get(i));
            }
        }
        if (!bpNode.isLeaf()) {
            //非叶子节点
            //count chilpageno1, childpageno2...
            writeTuple(genTupleInt(bpNode.getChildren().size()));
            for (int i = 0; i < bpNode.getChildren().size(); i++) {
                writeTuple(genTupleInt(bpNode.getChildren().get(i).getPageNo()));
            }
        } else {
            //叶子节点需要写前后节点页号
            if (bpNode.getPrevious() == null) {
                writeTuple(genTupleInt(-1));
            } else {
                writeTuple(genTupleInt(bpNode.getPrevious().getPageNo()));
            }
            if (bpNode.getNext() == null) {
                writeTuple(genTupleInt(-1));
            } else {
                writeTuple(genTupleInt(bpNode.getNext().getPageNo()));
            }
        }
    }

    /**
     * 生成一个元组，表示当前页是否是叶子节点
     *
     * @return
     */
    private Tuple genIsLeafTuple() {
        return genBoolTuple(bpNode.isLeaf());
    }

    private Tuple genBoolTuple(boolean b) {
        if (b) {
            return genTupleInt(1);
        } else {
            return genTupleInt(0);
        }
    }

    private Tuple genTupleInt(int i) {
        Value[] vs = new Value[1];
        ValueInt valueInt = new ValueInt(i);
        vs[0] = valueInt;
        return new Tuple(vs);
    }


    private Tuple genIsRootTuple() {
        return genBoolTuple(bpNode.isRoot());
    }


    private int getTupleInt(Tuple tuple) {
        return ((ValueInt) tuple.getValues()[0]).getInt();
    }

    private boolean getTupleBoolean(Tuple tuple) {
        int i = ((ValueInt) tuple.getValues()[0]).getInt();
        return i == 1;
    }

    /**
     * 获取初始可用空间大小
     *
     * @return
     */
    public int getInitFreeSpace() {
        if (bpNode.isLeaf()) {
            //是叶子节点需要多一个ItemConst INT占用
            return length - SystemConfig.DEFAULT_SPECIAL_POINT_LENGTH - ItemConst.INT_LENGHT * 7 - PageHeaderData.PAGE_HEADER_SIZE;
        } else {
            return length - SystemConfig.DEFAULT_SPECIAL_POINT_LENGTH - ItemConst.INT_LENGHT * 6 - PageHeaderData.PAGE_HEADER_SIZE;
        }
    }

    public int calculateRemainFreeSpace() {
        return getInitFreeSpace() - getContentSize();
    }

    /**
     * 获取当前页的元组占用空间
     *
     * @return
     */
    public int getContentSize() {
        int size = 0;
        //累加当前节点的所有元组占用空间
        for (Tuple key : bpNode.getEntries()) {
            size += Item.getItemLength(key);
        }
        if (!bpNode.isLeaf()) {
            //如果不是叶子节点，还要有子节点指针占用空间
            for (int i = 0; i < bpNode.getEntries().size() + 1; i++) {
                size += ItemConst.INT_LENGHT;
            }
        }
        return size;
    }
}
