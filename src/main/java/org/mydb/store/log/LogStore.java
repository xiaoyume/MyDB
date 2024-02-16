package org.mydb.store.log;

import org.mydb.store.fs.FileUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/16 14:36
 */
public class LogStore {
    private String logPath;
    private FileChannel channel;
    public LogStore(String logPath) {
        this.logPath = logPath;
    }
    public void open() throws Exception {
        channel = FileUtils.open(logPath);
    }
    public void close() throws Exception {
        FileUtils.closeFile(channel);
    }

    /**
     * 追加日志
     * @param buffer
     */
    public void append(ByteBuffer buffer){
        try {
            FileUtils.writeFully(channel,buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
