package org.mydb.transaction.rm;

import org.mydb.meta.Tuple;

import java.nio.ByteBuffer;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/2/16 20:30
 */
public class LogRecord {
    //日志记录头
    private LogRecordHeader header;
    //
    private int operation;
    //操作前的记录
    private Tuple before;
    //动作后的记录
    private Tuple after;
    public LogRecord(int txId, int operation, Tuple before, Tuple after){
        header.setTxId(txId);
        this.operation = operation;
        this.before = before;
        this.after = after;
        header.setLength(getLength());
    }
    public int getLength(){
        int length = LogRecordHeader.getLength();
        //
        length += 4;
        length += 4 + before.getLength();
        length += 4 + after.getLength();
        return length;
    }

    public ByteBuffer getBytes(){
        ByteBuffer buffer = ByteBuffer.allocate(getLength());
        buffer.put(header.getBytes());
        //
        if(before == null){
            //当前是第一个
            buffer.putInt(0);
        }else{
            //长度 +
            buffer.putInt(before.getLength());
            buffer.put(before.getBytes());
        }
        if(after == null){
            buffer.putInt(0);
        }else{
            buffer.putInt(after.getLength());
            buffer.put(after.getBytes());
        }
        return buffer;
    }
    public int getOperation(){
        return operation;
    }
    public LogRecord setOperation(int operation){
        this.operation = operation;
        return this;
    }
    public Tuple getBefore(){
        return before;
    }

    public LogRecord setBefore(Tuple before){
        this.before = before;
        return this;
    }
    public Tuple getAfter(){
        return after;
    }
    public LogRecord setAfter(Tuple after){
        this.after = after;
        return this;
    }
}
