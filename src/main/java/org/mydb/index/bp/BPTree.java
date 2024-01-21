package org.mydb.index.bp;

import org.mydb.index.BaseIndex;
import org.mydb.meta.Attribute;
import org.mydb.meta.Relation;
import org.mydb.meta.Tuple;
import org.mydb.meta.value.ValueInt;
import org.mydb.store.item.Item;
import org.mydb.store.page.Page;
import org.mydb.store.page.PageLoader;
import org.mydb.store.page.PagePool;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xiaoy
 * @version 1.0
 * @description: B+树结构
 * @date 2023/12/15 16:00
 */
public class BPTree extends BaseIndex {

    //根节点
    protected BPNode root;

    //叶子节点的链表头
    protected BPNode head;

    //节点映射
    protected Map<Integer, BPNode> nodeMap;

    public BPTree(Relation relation, String indexName, Attribute[] attributes){
        super(relation, indexName, attributes);
        root = new BPNode(true, true, this);
        head = root;
        nodeMap = new HashMap<>();
    }

    /**
     * 读取root节点
     */
    public void loadFromDisk() {
        int rootPageNo = getRootPageNoFromMeta();
        getNodeFromPageNo(rootPageNo);
    }

    /**
     * 从第0页的第0个tuple的第0个int值
     * @return
     */
    public int getRootPageNoFromMeta(){

        PageLoader loader = new PageLoader(fStore.readPageFromFile(0));
        loader.load();
        return ((ValueInt)loader.getTuples()[0].getValues()[0]).getInt();
    }

    /**
     * 根据页码获取节点对象
     * @param pageNo
     * @return
     */
    public BPNode getNodeFromPageNo(int pageNo){
        if(pageNo == -1){
            return null;
        }
        BPNode bpNode = nodeMap.get(pageNo);
        if(bpNode != null){
            return bpNode;
        }
        //尝试从文件中读取
        BPPage bpPage = (BPPage) fStore.readPageFromFile(pageNo, true, this);
        bpNode = bpPage.readFromPage(this);
        if(bpNode.isRoot()){
            root = bpNode;
        }
        //是叶子节点并且前面没有节点，那么这个节点就是head节点
        if(bpNode.isLeaf() && bpNode.getPrevious() == null){
            head = bpNode;
        }
        return bpNode;
    }
    @Override
    public void flushToDisk() {
        writeMetaPage();
        //
        root.flushToDisk(fStore);
    }

    public void writeMetaPage(){
        Page page = PagePool.getInstance().getFreePage();
        page.writeItem(new Item(BPPage.genTupleInt(root.getPageNo())));
        fStore.writePageToFile(page, 0);
    }

    @Override
    public Tuple get(Tuple key) {
        return root.get(key);
    }

    @Override
    public boolean remove(Tuple key) {
        return root.remove(key, this);
    }

    @Override
    public void insert(Tuple key) {
        root.insert(key, this);
    }

    public BPNode getRoot() {
        return root;
    }
    public BPTree setRoot(BPNode root) {
        this.root = root;
        return this;
    }

    public BPNode getHead() {
        return head;
    }
    public BPTree setHead(BPNode head) {
        this.head = head;
        return this;
    }
}
