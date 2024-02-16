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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
        BPPage bpPage = (BPPage) fStore.readPageFromFile(pageNo, true);
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
    public GetRes getFirst(Tuple key) {
        GetRes getRes = root.get(key);
        //存在key一样的情况，所以必须向前遍历
        BPNode bpNode = getRes.getBpNode().getPrevious();
        while(bpNode != null){
            //从后往前查找
            for(int i = bpNode.getEntries().size() - 1; i >= 0; i--){
                Tuple item = bpNode.getEntries().get(i);
                if(item.compareIndex(key) == 0){
                    getRes.setBpNode(bpNode);
                    getRes.setTuple(item);
                }
                if(!item.equals(key)){
                    break;
                }
            }
            bpNode = bpNode.getPrevious();
        }
        return getRes;
    }

    //遍历当前bpNode以及之后的node
    @Override
    public List<Tuple> getAll(Tuple key) {
        GetRes res = getFirst(key);
        List<Tuple> list = new ArrayList<>();
        BPNode bpNode = res.getBpNode();
        BPNode initNode = res.getBpNode();
        while(bpNode != null){
            for(Tuple tuple : bpNode.getEntries()){
                if(tuple.compareIndex(key) == 0){
                    list.add(tuple);
                }else{
                    //
                    if(initNode != bpNode){
                        break;
                    }
                }
            }
            bpNode = bpNode.getNext();
        }
        return list;
    }

    public Map<Integer, BPNode> getNodeMap(){
        return nodeMap;
    }

    public BPTree setNodeMap(Map<Integer, BPNode> nodeMap) {
        this.nodeMap = nodeMap;
        return this;
    }

    public boolean innerRemove(Tuple key){
        return root.remove(key, this);
    }
    @Override
    public int remove(Tuple key){
        int count = 0;
        while(true){
            if(!innerRemove(key)){
                break;
            }
            count ++;
        }
        return count;
    }

    @Override
    public boolean removeOne(Tuple key) {
        return innerRemove(key);
    }

    @Override
    public void insert(Tuple key, boolean isUnique) {
        root.insert(key, this, isUnique);
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
