package org.mydb.index.bp;

import org.mydb.constant.ItemConst;
import org.mydb.meta.Tuple;
import org.mydb.store.fs.FStore;
import org.mydb.store.item.Item;
import org.mydb.store.page.PageFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaoy
 * @version 1.0
 * @description: b+树节点
 * @date 2023/12/15 16:00
 */
public class BPNode {
    //是否是叶子节点
    protected boolean isLeaf;
    //是否是根节点
    protected boolean isRoot;
    //对应页号
    protected int pageNo;
    protected BPNode parent;
    //前节点
    protected BPNode previous;
    //后节点
    protected BPNode next;

    //节点关键字，一个bpnode其实就是一个页，entries就是页中的所有tuple（记录）
    protected List<Tuple> entries;
    protected List<BPNode> children;
    //页结构
    protected BPPage bpPage;
    //属于哪个b+树
    protected BPTree bpTree;

    public BPNode(boolean isLeaf, BPTree bpTree) {
        this.isLeaf = isLeaf;
        this.bpTree = bpTree;
        this.pageNo = bpTree.getNextPageNo();
        //默认root是false;
        entries = new ArrayList<>();
        if (!isLeaf) {
            children = new ArrayList<>();
        }
        bpPage = PageFactory.getInstance().newBPPage(this);
    }


    public BPNode(boolean isLeaf, boolean isRoot, BPTree bpTree) {
        this(isLeaf, bpTree);
        this.isRoot = isRoot;
    }


    public Tuple get(Tuple key) {
        if (isLeaf) {
            for (Tuple tuple : entries) {
                if (tuple.compare(key) == 0) {
                    return tuple;
                }
            }
            return null;
        } else {//非叶子节点
            //如果key<最左边的key,沿着第一个子节点继续搜索
            if (key.compare(entries.get(0)) < 0) {
                return children.get(0).get(key);

                //比最右边得到节点key大，最后一个节点搜索
            } else if (key.compare(entries.get(entries.size() - 1)) >= 0) {
                return children.get(children.size() - 1).get(key);
                //在中间
            } else {
                for (int i = 0; i < entries.size(); i++) {
                    //比key大的前一个子节点继续搜索
                    if (key.compare(entries.get(i)) >= 0 && key.compare(entries.get(i + 1)) < 0) {
                        return children.get(i + 1).get(key);
                    }
                }
            }
        }
        return null;
    }

    public void insert(Tuple key, BPTree tree) {
        if (getBorrowKeyLength(key) > bpPage.getInitFreeSpace() / 3) {
            throw new RuntimeException("key size must <= Max / 3");
        }
        //如果是叶子节点
        if (isLeaf) {
            //如果当前页空间足够，直接插入
            if (!isLeafSplit(key)) {
                innerInsert(key);//插入到entries里
            } else {//如果当前页空间不够，分裂
                //分裂成左右两个节点
                BPNode left = new BPNode(true, bpTree);//叶子节点，当前页结构
                BPNode right = new BPNode(true, bpTree);
                if (previous != null) {//不是页节点的第一个节点
                    previous.setNext(left);//前节点的下一个节点设置为left
                    left.setPrevious(previous);//left的前节点设置为previous
                }
                if (next != null) {
                    next.setPrevious(right);
                    right.setNext(next);
                }
                if (previous == null) {//如果为空，说明this是第一个节点
                    tree.setHead(left);//把left设置为头节点
                }
                left.setNext(right);
                right.setPrevious(left);

                //this.previous,next置空，需要回收当前节点
                previous = null;
                next = null;

                //插入后分裂，先插入到entries里，再分裂成两个
                innerInsert(key);
                int leftSize = this.entries.size() / 2;
                int rightSize = this.entries.size() - leftSize;

                //左右节点分别赋值
                for (int i = 0; i < leftSize; i++) {
                    left.getEntries().add(entries.get(i));
                }
                for (int i = 0; i < rightSize; i++) {
                    right.getEntries().add(entries.get(leftSize + i));
                }

                if (parent != null) {
                    //当前节点不是根节点
                    //寻找父子节点之间的关系
                    //找当前节点在父节点里的index
                    int index = parent.getChildren().indexOf(this);
                    //移除当前节点，并把分裂的左右节点添加到父节点中
                    parent.getChildren().remove(this);
                    left.setParent(parent);
                    right.setParent(parent);
                    parent.getChildren().add(index, left);
                    parent.getChildren().add(index + 1, right);
                    //回收节点页面
                    recycle();
                    //把右子节点的第一个索引关键字插入到父节点entries中，便于查找
                    parent.innerInsert(right.getEntries().get(0));
                    //更新
                    parent.updateInsert(tree);
                    setParent(null);
                } else {
                    //如果
                    isRoot = false;
                    //根节点分裂
                    BPNode parent = new BPNode(false, true, bpTree);
                    tree.setRoot(parent);
                    left.setParent(parent);
                    right.setParent(parent);
                    parent.getChildren().add(left);
                    parent.getChildren().add(right);
                    //
                    recycle();
                    parent.innerInsert(right.getEntries().get(0));
                    parent.updateInsert(tree);
                }
            }
        }else{
            //非叶子节点，沿着第一个子节点继续搜索,插入到子节点
            if(key.compare(entries.get(0)) < 0){//key<最左边的key,沿着第一个子节点继续搜索
                children.get(0).insert(key, tree);
            }else if(key.compare(entries.get(entries.size() - 1)) >= 0){
                //比最后一个节点大，沿着子节点最后一个插入
                children.get(children.size() - 1).insert(key, tree);
            }else{
                for(int i = 0; i < entries.size(); i++){
                    if(key.compare(entries.get(i)) >= 0 && key.compare(entries.get(i+1)) < 0){
                        children.get(i+1).insert(key, tree);
                        break;
                    }
                }
            }

        }
    }

    /**
     * 更新结构里的node，因为可能左右分裂后还是超出空间，所以又递归判断
     * 分裂作用
     * @param tree
     */
    private void updateInsert(BPTree tree) {
        if (isNodeSplit()) {//当前页面放不下，需要分裂
            BPNode left = new BPNode(false, bpTree);//非叶子节点
            BPNode right = new BPNode(false, bpTree);
            int leftSize = this.entries.size() / 2;
            int rightSize = this.entries.size() - leftSize;
            //左边复制entry
            for (int i = 0; i < leftSize; i++) {
                left.getEntries().add(entries.get(i));
            }
            //左复制child，多复制一个，因为右边第一个关键字要插入到父节点中
            for (int i = 0; i <= leftSize; i++) {
                left.getChildren().add(children.get(i));
                children.get(i).setParent(left);
            }

            //右边第一个关键字提到父节点，
            for (int i = 1; i < rightSize; i++) {
                right.getEntries().add(entries.get(leftSize + i));
            }
            for (int i = 1; i < rightSize + 1; i++) {
                right.getChildren().add(children.get(leftSize + i));
                children.get(leftSize + i).setParent(right);
            }
            Tuple keyToUpdateParent = entries.get(leftSize);//右1上提
            if (parent != null) {//非根
                int index = parent.getChildren().indexOf(this);
                parent.getChildren().remove(this);
                left.setParent(parent);
                right.setParent(parent);
                parent.getChildren().add(index, left);
                parent.getChildren().add(index + 1, right);
                //插入关键字
                parent.innerInsert(keyToUpdateParent);
                //父节点更新关键字
                parent.updateInsert(tree);//
                recycle();
            } else {//当前是根节点，所以需要创建一个根节点补上
                isRoot = false;
                //需要创建父节点
                BPNode parent = new BPNode(false, true, bpTree);
                tree.setRoot(parent);
                left.setParent(parent);
                right.setParent(parent);
                parent.getChildren().add(left);
                parent.getChildren().add(right);
                recycle();
                parent.innerInsert(keyToUpdateParent);
                parent.updateInsert(tree);
            }

        }
    }

    private boolean isNodeSplit() {
        if (bpPage.calculateRemainFreeSpace() < 0) {
            return true;
        }
        return false;
    }

    private void recycle() {
        setEntries(null);
        setChildren(null);
        bpTree.recyclePageNo(pageNo);
    }

    /**
     * 插入到当前节点的关键字中
     * 保证有序
     *
     * @param key
     */
    private void innerInsert(Tuple key) {
        //如果关键字列表长度为0，直接插入
        if (entries.size() == 0) {
            entries.add(key);
            return;
        }
        //否则遍历列表
        for (int i = 0; i < entries.size(); i++) {
            //如果关键字值已经存在，更新
            if (entries.get(i).compare(key) == 0) {
                return;
            } else if (entries.get(i).compare(key) > 0) {
                //插入
                if (i == 0) {
                    entries.add(0, key);
                    return;
                } else {
                    entries.add(i, key);
                    return;
                }
            }
        }
        //到这里说明遍历完了还没有，插入到末尾
        entries.add(entries.size(), key);
    }

    public boolean isLeafSplit(Tuple key) {
        //如何当前页面的剩余空间不足以插入key
        if (bpPage.calculateRemainFreeSpace() < Item.getItemLength(key)) {
            return true;
        }
        return false;
    }

    private int getBorrowKeyLength(Tuple key) {
        //如果是非叶子节点，需要额外的指针长度计数子节点数
        if (!isLeaf) {
            return Item.getItemLength(key) + ItemConst.INT_LENGHT;
        } else {
            return Item.getItemLength(key);
        }
    }

    /**
     * 根据关键字移除
     * @param key
     * @param tree
     * @return
     */
    protected boolean remove(Tuple key, BPTree tree) {
        boolean found = false;
        //如果是叶子节点
        if (isLeaf) {
            //如果不包含key，直接返回
            if(!contrains(key)){
                return false;
            }
            //包含了
            found = true;
            //表示节点同时也是根节点,直接移除
            if(isRoot){
                remove(key);
            }else{
                if(canRemoveDirect(key)){
                    remove(key);
                }else{//不满足移除后占用空间大于初始空间的一半
                    //先移除
                    remove(key);
                    //从前节点借 一个关键字
                    if(canLeafBorrowPrevious()){
                        borrowLeafPrevious();
                    }else if(canLeafBorrowNext()){
                        borrowLeafNext();
                    }else{//借不了，说明3个节点移除都会小于空间一半，所以要合并空间
                        if(canLeafMerge(previous)){
                            //与前节点合并
                            addPreNode(previous);
                            previous.recycle();
                            //删除父节点的entry,子节点少了一个，父节点关键字也要少一个
                            int curEntryIndex = getParentEntry(this);
                            if(parent == null || parent.getEntries() == null || curEntryIndex < 0){
                                curEntryIndex = getParentEntry(this);
                            }
                            parent.getEntries().remove(curEntryIndex);
                            //移除前节点
                            parent.getChildren().remove(previous);
                            previous.setParent(null);
                            //更新链表
                            if(previous.getPrevious() != null){
                                BPNode temp = previous;
                                temp.getPrevious().setNext(this);
                                previous = temp.getPrevious();
                                temp.setPrevious(null);
                                temp.setNext(null);
                            }else{
                                tree.setHead(this);
                                previous.setNext(null);
                                previous = null;
                            }
                        }else if(canLeafMerge(next)){
                            addNextNode(next);
                            next.recycle();
                            //移除后节点对应父节点中的entry
                            int curEntryIndex = getParentEntry(this.next);
                            parent.getEntries().remove(curEntryIndex);
                            parent.getChildren().remove(next);
                            //更新链表
                            if(next.getNext() != null){
                                BPNode temp = next;
                                temp.getNext().setPrevious(this);
                                next = temp.getNext();
                                temp.setPrevious(null);
                                temp.setNext(null);
                            }else{
                                next.setPrevious(null);
                                next = null;
                            }
                        }
                    }
                }
                //
                parent.updataRemove(tree);
            }
        }
        return found;
    }

    private void addNextNode(BPNode bpNode) {
        if(!bpNode.isLeaf()){
            //获取后节点在父节点关键字列表中的对应位置
            int parentIdx = this.getParentEntry(bpNode);
            //父节点关键字下移，非叶子节点才需要处理，叶子节点关键字里有
            entries.add(bpNode.getParent().getEntries().get(parentIdx));
        }
        for(int i = 0; i < bpNode.getEntries().size(); i++){
            entries.add(bpNode.getEntries().get(i));
        }
        //把后节点的子节点添加到当前节点的子节点列表中，处理节点关系
        if(bpNode.getChildren() != null){
            for(int i = 0; i < bpNode.getChildren().size(); i++){
                bpNode.getChildren().get(i).setParent(this);
                children.add(bpNode.getChildren().get(i));
            }
        }
    }

    /**
     * 合并前节点
     * 目的把前一个节点移除掉，关键字合并到当前节点，子节点也合并到当前节点，这里还没有处理父节点
     * @param bpNode
     */
    private void addPreNode(BPNode bpNode) {
        if(!bpNode.isLeaf()){
            //获取前节点在父节点关键字列表中的位置
            int parentIdx = this.getParentEntry(this);
            //父节点关键字下移
            entries.add(0, this.getParent().getEntries().get(parentIdx));
        }
        //前一节点的关键字添加到当前节点的关键字列表中
        for(int i = bpNode.getEntries().size() - 1; i >= 0; i--){
            entries.add(0, bpNode.getEntries().get(i));
        }
        if(!bpNode.isLeaf()){
            for(int i = bpNode.getChildren().size()-1; i >= 0; i--){
                //把前节点的所有子节点的父节点设置为当前节点
                bpNode.getChildren().get(i).setParent(this);
                children.add(0, bpNode.getChildren().get(i));
            }
        }
    }

    /**
     * 判断当前节点能否和输入节点合并,
     * 满足 当前节点占用空间小于初始空间的一半
     * 占用空间要小于页面剩余空间+key占用空间，（便于合并）
     * 并且节点的父节点是当前节点的父节点
     * 那么可以合并
     * @param bpNode
     * @return
     */
    private boolean canLeafMerge(BPNode bpNode) {
        if(bpNode == null){
            return false;
        }
        if(bpNode.bpPage.getContentSize() < bpNode.bpPage.getInitFreeSpace() / 2 && bpNode.bpPage
                .getContentSize() <= bpPage.calculateRemainFreeSpace() && bpNode.getParent() == parent){
            return true;
        }
        return false;
    }

    /**
     * 从后一个节点借一个关键字，跟新父节点的关键字，此节点和后一个节点的关键字
     */
    private void borrowLeafNext() {
        Tuple borrowKey = next.getEntries().get(0);
        next.getEntries().remove(borrowKey);
        entries.add(borrowKey);
        //
        int curEntryIndex = getParentEntry(this.next);
        parent.getEntries().remove(curEntryIndex);
        //将更新后的第一个往上提
        parent.getEntries().add(curEntryIndex, next.getEntries().get(0));
    }

    /**
     * 获取node在父节点中的位置，返回的是当前节点在父节点中的位置，n个entry对应n+1个child所以要-1
     * @param bpNode
     * @return
     */
    private int getParentEntry(BPNode bpNode) {
        int index = parent.getChildren().indexOf(bpNode);
        return index - 1;
    }

    private boolean canLeafBorrowNext() {
        if(next != null && next.getEntries().size() > 2 && next.getParent() == parent){
            Tuple borrowKey = next.getEntries().get(0);
            int borrowKeyLength = getBorrowKeyLength(borrowKey);
            //借完后占用空间大于初始空间的一半
            if((next.bpPage.getContentSize() - borrowKeyLength) > next.bpPage.getInitFreeSpace() / 2){
                return true;
            }
        }
        return false;
    }

    /**
     * 从前一个节点借一个key过来
     */
    private void borrowLeafPrevious() {
        int size = previous.getEntries().size();
        //从previous接最后一个过来，加到当前entry前
        Tuple borrowKey = previous.getEntries().get(size-1);
        previous.getEntries().remove(borrowKey);
        entries.add(0, borrowKey);
        //找到当前节点在父节点中的entries
        int curEntryIndex = parent.getChildren().indexOf(this);
        parent.getEntries().remove(curEntryIndex);
        parent.getEntries().add(curEntryIndex, borrowKey);
    }

    /**
     * 判断能否从当前前一个节点借一个key出来
     * @return
     */
    private boolean canLeafBorrowPrevious() {
        if(previous != null && previous.getEntries().size() > 2 && previous.getParent() == parent){
            Tuple borrowKey = previous.getEntries().get(previous.getEntries().size()-1);
            int borrowKeyLength = getBorrowKeyLength(borrowKey);
            //借完后占用空间要大于1半
            if((previous.bpPage.getContentSize() - borrowKeyLength) > previous.bpPage.getInitFreeSpace() / 2){
                return true;
            }
        }
        return false;
    }

    /**
     * 判断是否可以直接删除
     * 规定移除后的占用空间要大于初始空间的一半才可以移除，这个是为了保证每个页面的占用率
     * @param key
     * @return
     */
    private boolean canRemoveDirect(Tuple key) {
        if((bpPage.getContentSize() - Item.getItemLength(key)) > bpPage.getInitFreeSpace() / 2){
            return true;
        }
        return false;
    }

    /**
     * 删除关键字
     * @param key
     * @return
     */
    private boolean remove(Tuple key) {
        int index = -1;
        boolean found = false;
        //先找到位置
        for(int i = 0; i < entries.size(); i++){
            if(entries.get(i).compare(key) == 0){
                index = i;
                found = true;
                break;
            }
        }
        if(index != -1){
            entries.remove(index);
        }
        return found;
    }

    /**
     * 判断叶子节点是否包含此key
     * @param key
     * @return
     */
    private boolean contrains(Tuple key) {
        for(Tuple item : entries){
            if(item.compare(key) == 0){
                return true;
            }
        }
        return false;
    }

    /**
     * 删除key后的节点更新
     * 页面空间太小
     *
     * @param tree
     */
    protected void updataRemove(BPTree tree) {
        //children小于2或者占用空间小于初始空间一半，才处理
        if(children.size() < 2 || bpPage.getContentSize() < bpPage.getInitFreeSpace() / 2){
            //如果当前是根节点
            if(isRoot){
              //如果子节点>=2,不需要处理
              if(children.size() >= 2){
                  return;
              }else{//小于2，根节点大小一点是小于叶子节点的，直接是用叶子节点替代根节点
                  //当前节点和子节点合并
                  //将子节点作为根节点
                  //导致根节点的pageNo不为0
                  BPNode root = children.get(0);
                  tree.setRoot(root);
                  root.setParent(null);
                  root.setRoot(true);
                  recycle();//回收当前节点
              }
            }else{
                //计算前后节点
                int curIndex = parent.getChildren().indexOf(this);//当前节点是父节点的子节点的第几个
                int preIndex = curIndex - 1;
                int nextIndex = curIndex + 1;
                BPNode previous = null, next = null;
                if(preIndex >= 0){//存在前节点
                    previous = parent.getChildren().get(preIndex);
                }
                if(nextIndex < parent.getChildren().size()){
                    next = parent.getChildren().get(nextIndex);
                }
                if(canNodeBorrowPrevious(previous)){
                    //从前节点借
                    //
                    borrowNodePrevious(previous);
                    //判断借完之后是否超出空间
                    if(bpPage.getContentSize() > bpPage.getInitFreeSpace()){
                        System.out.println("size caculate error, contentSize=" + bpPage.getContentSize()
                        + ", freeSpace=" + bpPage.getInitFreeSpace());
                        throw new RuntimeException("size caculate error!");
                    }
                }else if(canNodeBorrowNext(next)){
                    //从后节点借
                    borrowNodeNext(next);
                    if(bpPage.getContentSize() > bpPage.getInitFreeSpace()){
                        System.out.println("size caculate error, contentSize=" + bpPage.getContentSize()
                                + ", freeSpace=" + bpPage.getInitFreeSpace());
                        throw new RuntimeException("size caculate error!");
                    }
                }else{
                    //需要合并节点
                    if(canMergePrevious(previous)){
                        //与前节点合并
                        addPreNode(previous);
                        previous.recycle();
                        // 移除父节点对应的key
                        //   10         14
                        // 1     11 12     15
                        //变成
                        //    10
                        // 1      11 12 14 15
                        int curEntryIndex = getParentEntry(this);
                        parent.getChildren().remove(curEntryIndex);
                        //
                        parent.getChildren().remove(previous);
                    }else if(canMergeNext(next)){
                        //
                        addNextNode(next);
                        next.recycle();
                        int curEntryIndex = getParentEntry(next);
                        parent.getEntries().remove(curEntryIndex);
                        parent.getChildren().remove(next);
                    }
                }
                //父节点做出改变后，父节点更新
                parent.updataRemove(tree);
            }
        }else if(this.bpPage.getContentSize() > this.bpPage.getInitFreeSpace()){
            //可能在更新的时候出现虽然删除了关键字，但是更新了新的长key，导致比删除之前的size还大，可能导致分裂
            //即changekeysize - deletekeysize > 0
            updateInsert(bpTree);
        }
    }

    /**
     * 判断能否和下一个节点合并
     * 判断父节点下移的key+后节点占用空间是否小于当前节点剩余空间
     * @param next
     * @return
     */
    private boolean canMergeNext(BPNode next) {
        if(next == null){
            return false;
        }
        if(next.getParent() == parent){
            int adjSize = 0;
            //叶子节点不用下移key,因为叶子节点里有
            if(!next.isLeaf()){
                int parentIdx = this.getParentEntry(next);
                Tuple downKey = this.getParent().getEntries().get(parentIdx);
                adjSize = Item.getItemLength(downKey);
            }
            if(next.bpPage.getContentSize() + adjSize <= bpPage.calculateRemainFreeSpace()){
                return true;
            }
        }
        return false;
    }

    private boolean canMergePrevious(BPNode previous) {
        if(previous == null){
            return false;
        }
        if(previous.getParent() == parent){
            int adjSize = 0;
            if(!previous.isLeaf()){
                int parentIdx = this.getParentEntry(this);
                //父节点的key下移一个
                Tuple downKey = this.getParent().getEntries().get(parentIdx);
                adjSize = Item.getItemLength(downKey);
            }
            //前节点的占用空间加一个父节点下移的key空间不能超过剩余空间
            if(previous.bpPage.getContentSize() + adjSize <= bpPage.calculateRemainFreeSpace()){
                return true;
            }
        }
        return false;
    }

    private void borrowNodeNext(BPNode next) {
        //    10
        //3        11   12
        //变为
        //     11
        //3 10    12
        int parentIdx = getParentEntry(next);
        //先下放
        Tuple downKey = parent.getEntries().get(parentIdx);
        entries.add(downKey);
        //next上提
        parent.getEntries().remove(parentIdx);
        parent.getEntries().add(parentIdx, next.getEntries().get(0));
        next.getEntries().remove(0);
        //child也拿过来
        BPNode borrowChild = next.getChildren().get(0);
        children.add(borrowChild);
        borrowChild.setParent(this);
        next.getChildren().remove(0);
    }

    private boolean canNodeBorrowNext(BPNode next) {
        if(next == null || next.getEntries().size() < 2){
            return false;
        }
        return canNodeBorrow(next, next.getEntries().get(0));
    }

    private void borrowNodePrevious(BPNode previous) {
        int size = previous.getEntries().size();
        int childSize = previous.getChildren().size();
        //将previous的最后一个entry到parent对应index下一个指向，再将父节点对应的entry下放到当前节点
        //      10
        //3 9       11
        //变为
        //     9
        //3          10 11
        int parentIdx = getParentEntry(previous) + 1;
        //
        Tuple downKey = parent.getEntries().get(parentIdx);
        //
        entries.add(0, downKey);
        //previous的key上提
        parent.getEntries().remove(parentIdx);
        parent.getEntries().add(parentIdx, previous.getEntries().get(size-1));
        previous.getEntries().remove(size-1);
        //child也拿过来
        BPNode borrowChild = previous.getChildren().get(childSize-1);
        children.add(0, borrowChild);
        borrowChild.setParent(this);
        previous.getChildren().remove(borrowChild);
    }

    /**
     * 判断是否可以从前一个节点借一个key过来
     * @param bpNode
     * @return
     */
    private boolean canNodeBorrowPrevious(BPNode bpNode) {
        if(bpNode == null || bpNode.getEntries().size() < 2){
            return false;
        }
        return canNodeBorrow(bpNode, bpNode.getEntries().get(bpNode.getEntries().size()-1));
    }

    private boolean canNodeBorrow(BPNode bpNode, Tuple tuple) {
        if(bpNode == null){
            return false;
        }
        int borrowKeyLength = getBorrowKeyLength(tuple);
        //前一个节点的占用空间要大于1半,借用key的大小要小于当前页剩余空间
        if(bpNode.parent == parent && bpNode.getEntries().size() >= 2 &&
            bpNode.bpPage.getContentSize() - borrowKeyLength > bpNode.bpPage.getInitFreeSpace() / 2
            && borrowKeyLength <= this.bpPage.calculateRemainFreeSpace()){
            return true;
        }else if(this.entries.size() == 0 && bpNode.getEntries().size() >= 2){
            //这里不检查borrowkeylength <= remainfreespace的原因，限定了key <= max/3，直接删除的条件是contentsize <= M/2
            //所以contentsize + key <= 5/6 M，不会移除
            return true;
        }else {
            return false;
        }
    }


    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public void setRoot(boolean root) {
        isRoot = root;
    }

    public int getPageNo() {
        return pageNo;
    }

    public BPNode setPageNo(int pageNo) {
        this.pageNo = pageNo;
        return this;
    }

    public BPNode getParent() {
        return parent;
    }

    public void setParent(BPNode parent) {
        this.parent = parent;
    }

    public BPNode getPrevious() {
        return previous;
    }

    public void setPrevious(BPNode previous) {
        this.previous = previous;
    }

    public BPNode getNext() {
        return next;
    }

    public void setNext(BPNode next) {
        this.next = next;
    }

    public List<Tuple> getEntries() {
        return entries;
    }

    public void setEntries(List<Tuple> entries) {
        this.entries = entries;
    }

    public List<BPNode> getChildren() {
        return children;
    }

    public void setChildren(List<BPNode> children) {
        this.children = children;
    }

    public BPPage getBpPage() {
        return bpPage;
    }

    public BPNode setBpPage(BPPage bpPage) {
        this.bpPage = bpPage;
        return this;
    }

    public BPTree getBpTree() {
        return bpTree;
    }

    public BPNode setBpTree(BPTree bpTree) {
        this.bpTree = bpTree;
        return this;
    }
    public void flushToDisk(FStore fStore){
        bpPage.writeToPage();
        fStore.writePageToFile(bpPage,pageNo);
        if(isLeaf){
            return;
        }
        for(BPNode bpNode : children){
            bpNode.flushToDisk(fStore);
        }
    }
}
