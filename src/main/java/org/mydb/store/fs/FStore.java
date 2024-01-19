package org.mydb.store.fs;

import org.mydb.config.SystemConfig;
import org.mydb.index.bp.BPPage;
import org.mydb.index.bp.BPTree;
import org.mydb.store.page.Page;
import org.mydb.store.page.PageLoader;
import org.mydb.store.page.PagePool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 文件存储
 * @date 2023/12/1 15:03
 */
public class FStore {
    //文件路径
    private String filePath;
    //文件传输通道
    private FileChannel fileChannel;
    //读写游标
    private long curFilePos;
    public FStore(String filePath) {
        this.filePath = filePath;
        this.curFilePos = 0;
    }
    //给文件通道赋值，打开文件
    public void open(){
        fileChannel = FileUtils.open(filePath);
    }
    public void writePageToFile(Page page,int pageIndex){
        try{
            int writePos = pageIndex * SystemConfig.DEFAULT_PAGE_SIZE;
            ByteBuffer buffer = ByteBuffer.wrap(page.getBuffer());
            FileUtils.writeFully(fileChannel, buffer, writePos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public PageLoader readPageLoaderFromFile(int pageIndex){
        Page page = readPageFromFile(pageIndex);
        PageLoader loader = new PageLoader(page);
        loader.load();
        return loader;
    }

    /**
     * 从文件中读取一个page
     * @param pageIndex
     * @return
     */
    public Page readPageFromFile(int pageIndex){
        return readPageFromFile(pageIndex, false, null);
    }

    /**
     *
     * @param pageIndex
     * @param isIndex
     * @param bpTree
     * @return
     */
    public Page readPageFromFile(int pageIndex, boolean isIndex, BPTree bpTree){
        int readPos = pageIndex * SystemConfig.DEFAULT_PAGE_SIZE;//和前面写入的位置对应
        ByteBuffer buffer = ByteBuffer.allocate(SystemConfig.DEFAULT_PAGE_SIZE);
        try {
            //读出字节流
            FileUtils.readFully(fileChannel, buffer, readPos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //bytebuffer转buffer
        byte[] b = new byte[SystemConfig.DEFAULT_PAGE_SIZE];
        buffer.flip();
        buffer.get(b);
        if(!isIndex){
            //非索引页
            Page page = PagePool.getInstance().getFreePage();
            page.read(b);
            return page;
        }else{
            BPPage bpPage = new BPPage(SystemConfig.DEFAULT_PAGE_SIZE);
            bpPage.read(b);
            return bpPage;
        }
    }

    public void close(){
        FileUtils.closeFile(fileChannel);
    }

}
