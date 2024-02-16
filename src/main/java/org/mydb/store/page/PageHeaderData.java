package org.mydb.store.page;

import org.mydb.config.SystemConfig;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 页头数据，保存了一些数据，开始的8个字节固定的，表示特殊标识，后面4个字节表示lowerOffset，后4个字节表示upperOffset，后4个字节表示special，后4个字节表示tupleCount，总共24个字节
 * @date 2023/12/1 16:11
 */
public class PageHeaderData {
    public static final Integer PAGE_HEADER_SIZE = 24;
    //page开头的magic word
    private String magicWord = "Session";
    //free space 的起始偏移
    private int lowerOffset;
    //指向pageheader种的loweroffset的起始位置
    public static final int LOWER_POINTER = 8;
    //free space 的结束偏移
    private int upperOffset;
    //指向pageHeader中的upperOffset起始位置
    public static final int UPPER_POINTER = 12;
    //special space的起始偏移
    private int special;
    //指向pageHeader中的special起始位置
    public static final int SPECIAL_POINTER = 16;
    //记录元组数量
    private int tupleCount;
    public static final int TUPLE_COUNT_POINTER = 20;
    //记录header长度
    private int headerLength;
    public PageHeaderData(int size){
        int magicWordLength = magicWord.getBytes().length + 1;
        //加4个4字节是表示存储几个值 loweroffset-upperoffset之间的空间是free空间
        lowerOffset = magicWordLength + 4 + 4 + 4 + 4;
        //留出special 空间 64，其它的是free space
        upperOffset = size - SystemConfig.DEFAULT_SPECIAL_POINT_LENGTH;
        special = upperOffset;
        headerLength = lowerOffset;
    }

    public void modifyLowerOffset(int i, Page page){
        lowerOffset = i;
        page.writeIntPos(i, LOWER_POINTER);
    }

    public void modifyUpperOffset(int i, Page page){
        upperOffset = i;
        page.writeIntPos(i, UPPER_POINTER);
    }

    public void modifySpecial(int i, Page page){
        special = i;
        page.writeIntPos(i, SPECIAL_POINTER);
    }

    /**
     * 元组数加1
     * @param page
     */
    public void addTupleCount(Page page){
        int count = page.readIntPos(TUPLE_COUNT_POINTER);
        count++;
        page.writeIntPos(count, TUPLE_COUNT_POINTER);
        tupleCount = count;
    }

    public void decTupleCount(Page page){
        int count = page.readIntPos(TUPLE_COUNT_POINTER);
        count--;
        page.writeIntPos(count, TUPLE_COUNT_POINTER);
        tupleCount = count;
    }
    void write(Page page){
        page.writeStringWithNull(magicWord);
        page.writeInt(lowerOffset);
        page.writeInt(upperOffset);
        page.writeInt(special);
        page.writeInt(tupleCount);
    }

    public static PageHeaderData read(Page page){
        PageHeaderData pageHeaderData = new PageHeaderData(page.getLength());
        pageHeaderData.magicWord = page.readStringWithNull();
        pageHeaderData.lowerOffset = page.readInt();
        pageHeaderData.upperOffset = page.readInt();
        pageHeaderData.special = page.readInt();
        pageHeaderData.tupleCount = page.readInt();
        return pageHeaderData;
    }

    public int getUpperOffset(){
        return upperOffset;
    }
    public PageHeaderData setUpperOffset(int upperOffset) {
        this.upperOffset = upperOffset;
        return this;
    }
    public int getLowerOffset() {
        return lowerOffset;
    }

    public PageHeaderData setLowerOffset(int lowerOffset) {
        this.lowerOffset = lowerOffset;
        return this;
    }

    public int getSpecial() {
        return special;
    }
    public PageHeaderData setSpecial(int special) {
        this.special = special;
        return this;
    }
    public int getTupleCount() {
        return tupleCount;
    }
    public int getLength(){
        return headerLength;
    }




}
