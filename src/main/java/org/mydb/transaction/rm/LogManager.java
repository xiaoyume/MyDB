package org.mydb.transaction.rm;

import org.mydb.store.log.LogStore;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/16 20:29
 */
public class LogManager {
    private LogStore logStore;
    private LSN maxLsn;
    private List<LogRecord> unReLog = new ArrayList<>();
    public void load(){
        //从logstore中读取文件
    }
    public LogRecord read(LSN lsn){
        return null;
    }
    public void insert(LogRecord logRecord){
        unReLog.add(logRecord);
    }
    //
    public LSN flush(){
        //刷日志
        //flush write
        return null;
    }
}
