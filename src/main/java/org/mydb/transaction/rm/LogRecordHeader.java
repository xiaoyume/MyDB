package org.mydb.transaction.rm;

import java.nio.ByteBuffer;
import java.util.Date;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 日志记录头
 * @date 2024/2/16 20:30
 */
public class LogRecordHeader {
    //当前record 总长，
    private int length;
    private LSN lsn;
    //暂时不用
    private LSN prevLsn;
    private  Long timeStamp = (new Date()).getTime();
    //写此条log记录的资源管理器id
    //不用
    private int rmId;
    //写当前记录的事务id
    private int txId;
    //该事务的前一个日志记录lsn
    private LSN tranPrevLsn;

    public ByteBuffer getBytes(){
        ByteBuffer byteBuffer = ByteBuffer.allocate(256);
        byteBuffer.putInt(length);
        byteBuffer.putInt(lsn.getRba());
        //pre lsn
        byteBuffer.putInt(0);//prevlsn.getRba();
        byteBuffer.putLong(timeStamp);
        byteBuffer.putInt(rmId);
        byteBuffer.putInt(txId);
        //前一个事务的lsn
        byteBuffer.putInt(tranPrevLsn.getRba());
        return byteBuffer;
    }
    public int getTxId() {
        return txId;
    }
    public LogRecordHeader setTxId(int txId) {
        this.txId = txId;
        return this;
    }
    public LogRecordHeader setLength(int length) {
        this.length = length;
        return this;
    }
    public static int getLength(){
        return 4 + 4 + 4 + 8 + 4 + 4 + 4;
    }
}
