package org.mydb.meta;

import org.mydb.config.SystemConfig;
import org.mydb.index.BaseIndex;
import org.mydb.meta.value.Value;
import org.mydb.meta.value.ValueInt;
import org.mydb.store.fs.FStore;
import org.mydb.store.item.Item;
import org.mydb.store.page.Page;
import org.mydb.store.page.PageLoader;
import org.mydb.store.page.PagePool;
import org.mydb.utils.ValueConverUtil;

import java.sql.Time;
import java.util.*;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 关系表
 * @date 2023/12/22 20:58
 */
public class Relation {
    //元组描述
    private TupleDesc tupleDesc;
    //filepath
    private String relPath;
    //metapath
    private String metaPath;
    //page数量默认为1
    private int pageCount = 1;
    //pagecount是否发生变化，如则更新pageoffset信息
    private boolean isPageCountDirty;
    private FStore relStore;
    private FStore metaStore;
    //页号偏移映射
    private Map<Integer, Integer> pageOffsetMap;
    //页号 pageload映射
    private Map<Integer, Page> pageMap;
    //索引集合，包含主键索引
    private List<BaseIndex> indexs = new ArrayList<>();
    //主键 聚簇索引
    //最后两个必须是pageNo, countNo
    private BaseIndex primaryIndex;
    public static final int META_PAGE_INDEX = 0;
    public static final int PAGE_OFFSET_INDEX = 0;

    public Relation() {

    }

    public void insert(Tuple tuple) {
        insert(new Item(tuple));
    }

    public void delete(Tuple tuple) {
        //直接删除会留下空洞，需要利用数据check
        //只有从主键索引查出来的才能删除
        //从二级索引查出来需要在主键索引里查出来，才能知道pageNo和pageCount
        Page page = relStore.readPageFromFile(getPageNo(tuple));
        page.delete(getPageCount(tuple));
    }

    /**
     * 主键索引的最后两个是pageCount和pageNo，取出tuple里的倒数第二个数据即可
     *
     * @param tuple
     * @return
     */
    public int getPageNo(Tuple tuple) {
        int length = tuple.getLength();
        return ((ValueInt) (tuple.getValues()[length - 2])).getInt();
    }

    /**
     * 同理，获取pageCount
     *
     * @param tuple
     * @return
     */
    public int getPageCount(Tuple tuple) {
        int length = tuple.getLength();
        return ((ValueInt) (tuple.getValues()[length - 1])).getInt();
    }

    /**
     * 删除旧tuple,插入新tuple
     * @param before
     * @param after
     */
    public void update(Tuple before, Tuple after) {
        delete(before);
        insert(after);
    }

    /**
     * 初始化两个文件读写通道，初始化页号偏移映射， 页号 pageload映射
     */
    public void open() {
        relStore = new FStore(relPath);
        relStore.open();
        metaStore = new FStore(metaPath);
        metaStore.open();
        //
        pageOffsetMap = new HashMap<Integer, Integer>();
        pageMap = new HashMap<Integer, Page>();
    }

    /**
     * 插入数据到表里
     *
     * @param item
     */
    public void insert(Item item) {
        int itemLength = item.getLength();
        int pageNo = findEnoughSpace(itemLength);
        if (pageNo > 0) {
            pageMap.get(pageNo).writeItem(item);
            return;
        }
        //没有空闲空间，则新建页
        Page page = mallocPage();
        if (page.remainFreeSpace() < itemLength) {
            throw new RuntimeException("item size too long");
        }
        page.writeItem(item);
    }

    public void loadFromDisk() {
        readMeta();
        readPageOffInfo();
        pageMap.clear();
        pageMap.clear();
        //通过pageoffset重新加载数据
        for (int pageNo : pageOffsetMap.keySet()) {
            pageMap.put(pageNo, relStore.readPageFromFile(pageNo));
        }
    }

    public void flushToDisk() {
        if (isPageCountDirty) {
            //pagecount变化，则更新pageoffset
            writePageOffInfo();
        }
        for (Integer pageNo : pageMap.keySet()) {
            Page page = pageMap.get(pageNo);
            if (page.isDirty()) {
                relStore.writePageToFile(page, pageNo);
                //page写完后，page变为clean
                page.setDirty(false);
            }
        }

    }


    /**
     * 写入page offset映射信息
     */
    public void writePageOffInfo() {
        List<Item> list = new ArrayList<>();
        for (Integer pageNo : pageOffsetMap.keySet()) {
            Value[] values = new Value[2];
            values[0] = new ValueInt(pageNo);
            values[1] = new ValueInt(pageOffsetMap.get(pageNo));
            Tuple tuple = new Tuple(values);
            list.add(new Item(tuple));
        }
        Page page = PagePool.getInstance().getFreePage();
        page.writeItems(list);
        relStore.writePageToFile(page, PAGE_OFFSET_INDEX);
    }


    /**
     * 读取页pageoffset映射信息 pageNo, offset
     */
    public void readPageOffInfo() {
        //pageoffset只在第一页有
        PageLoader loader = relStore.readPageLoaderFromFile(0);
        pageOffsetMap.clear();
        pageCount = loader.getTuples().length;
        for (Tuple tuple : loader.getTuples()) {
            Value[] values = tuple.getValues();
            int pageNo = ((ValueInt) values[0]).getInt();
            int pageoffset = ((ValueInt) values[1]).getInt();
            pageOffsetMap.put(pageNo, pageoffset);
        }
    }

    /**
     * 读取元数据
     */
    public void readMeta() {
        PageLoader loader = metaStore.readPageLoaderFromFile(0);
        List<Attribute> list = new LinkedList<>();
        for (Tuple tuple : loader.getTuples()) {
            Attribute attr = ValueConverUtil.convertValue(tuple.getValues());
            list.add(attr);
        }
        //list转为数组
        tupleDesc = new TupleDesc(list.toArray(new Attribute[list.size()]));
    }

    public void writeMeta() {
        Page page = convertMetaToPage();
        metaStore.writePageToFile(page, META_PAGE_INDEX);
    }

    /**
     * 元数据转为page
     *
     * @return
     */
    private Page convertMetaToPage() {
        //首先获取需要写入的信息
        List<Item> list = tupleDesc.getItems();
        Page page = PagePool.getInstance().getFreePage();
//        pageMap.put(pageCount, page);
//        pageOffsetMap.put(pageCount, pageCount * SystemConfig.DEFAULT_PAGE_SIZE);
        pageCount++;
        page.writeItems(list);
        return page;
    }


    private int findEnoughSpace(int needSpace) {
        //遍历每一页，看看有无空闲空间
        for (Integer pageNo : pageMap.keySet()) {
            if (pageMap.get(pageNo).remainFreeSpace() >= needSpace) {
                return pageNo;
            }
        }
        return -1;
    }

    private Page mallocPage() {
        Page page = PagePool.getInstance().getFreePage();
        pageMap.put(pageCount, page);
        pageOffsetMap.put(pageCount, pageCount * SystemConfig.DEFAULT_PAGE_SIZE);
        incrPageCount();
        return page;
    }

    public void incrPageCount() {
        pageCount++;
        isPageCountDirty = true;
    }

    public String getRelPath() {
        return relPath;
    }

    public Relation setRelPath(String relPath) {
        this.relPath = relPath;
        return this;
    }

    public String getMetaPath() {
        return metaPath;
    }

    public Relation setMetaPath(String metaPath) {
        this.metaPath = metaPath;
        return this;
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public Relation setTupleDesc(TupleDesc tupleDesc) {
        this.tupleDesc = tupleDesc;
        return this;
    }

    public void close() {
        relStore.close();
        metaStore.close();
    }

    public Map<Integer, Page> getPageMap() {
        return pageMap;
    }

    public Relation setPageMap(Map<Integer, Page> pageMap) {
        this.pageMap = pageMap;
        return this;
    }

    public int getPageCount() {
        return pageCount;
    }

    public Relation setPageCount(int pageCount) {
        this.pageCount = pageCount;
        return this;
    }

    public List<BaseIndex> getIndexs() {
        return indexs;
    }
    public Relation setIndexs(List<BaseIndex> indexs) {
        this.indexs = indexs;
        return this;
    }

}
