package org.mydb.utils;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 处理字节缓冲区
 * @date 2023/11/27 19:57
 */
public class BufferWrapper {
    private byte[] buffer;
    //写索引
    private int writeIndex;
    //读索引
    private int readIndex;
    private int length;

    public BufferWrapper(int size) {
        this.buffer = new byte[size];
        this.writeIndex = 0;
        this.readIndex = 0;
        this.length = buffer.length;
    }
    public BufferWrapper(byte[] buffer){
        this.buffer = buffer;
        writeIndex = 0;
        readIndex = 0;
        length = buffer.length;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    /**
     * 把一个int类型写入byte缓冲区
     * 从低位到高位依次写入字节
     * @param i
     */
    public void writeInt(int i){
        buffer[writeIndex++] = (byte) (i & 0xFF);//得到低8位的值
        buffer[writeIndex++] = (byte) ((i >> 8) & 0xFF);
        buffer[writeIndex++] = (byte) ((i >> 16) & 0xFF);
        buffer[writeIndex++] = (byte) ((i >> 24) & 0xFF);//高8位
    }

    /**
     * 从指定位置写入int
     * @param i
     * @param pos
     */
    public void writeIntPos(int i, int pos){
        buffer[pos++] = (byte) (i & 0xFF);//得到低8位的值
        buffer[pos++] = (byte) ((i >> 8) & 0xFF);
        buffer[pos++] = (byte) ((i >> 16) & 0xFF);
        buffer[pos++] = (byte) ((i >> 24) & 0xFF);//高8位
    }
    public void writeByte(byte i){
        buffer[writeIndex++] = i;
    }

    /**
     * 把long类型写入byte缓冲区
     * @param i
     */
    public void writeLong(long i){
        buffer[writeIndex++] = (byte) (i & 0xFF);
        buffer[writeIndex++] = (byte) ((i >> 8) & 0xFF);
        buffer[writeIndex++] = (byte) ((i >> 16) & 0xFF);
        buffer[writeIndex++] = (byte) ((i >> 24) & 0xFF);
        buffer[writeIndex++] = (byte) ((i) >> 32 & 0xFF);
        buffer[writeIndex++] = (byte) ((i) >> 40 & 0xFF);
        buffer[writeIndex++] = (byte) ((i) >> 48 & 0xFF);
        buffer[writeIndex++] = (byte) ((i) >> 56 & 0xFF);
    }

    /**
     * bytes写入到buffer里，从writeIndex开始写入
     * @param bytes
     */
    public void writeBytes(byte[] bytes){
        System.arraycopy(bytes, 0, buffer, writeIndex, bytes.length);
    }

    /**
     * 从pos开始写
     * @param src
     * @param pos
     */
    public void writeBytes(byte[] src, int pos){
        System.arraycopy(src, 0, buffer, pos, src.length);
    }
    public void writeWithNull(byte[] src){
        System.arraycopy(src, 0, buffer, writeIndex, src.length);
        writeIndex += src.length;
        //写入结束符
        buffer[writeIndex++] = 0;
    }
    public void writeStringWithNull(String s){
        writeWithNull(s.getBytes());
    }

    /**
     * 读4字节的int数据
     * @return
     */
    public int readInt(){
        final byte[] b = this.buffer;
        int i = buffer[readIndex++] & 0xFF;
        i |= (buffer[readIndex++] & 0xFF) << 8;
        i |= (buffer[readIndex++] & 0xFF) << 16;
        i |= (buffer[readIndex++] & 0xFF) << 24;
        return i;
    }

    public int readIntPos(int pos){
        final byte[] b = this.buffer;
        int i = buffer[pos++] & 0xff;
        i |= (buffer[pos++] & 0xff) << 8;
        i |= (buffer[pos++] & 0xff) << 16;
        i |= (buffer[pos++] & 0xff) << 24;
        return i;
    }
    public long readLong(){
        final byte[] b = this.buffer;
        long l = (long)(b[readIndex++] & 0xFF);
        l |= (long)(b[readIndex++] & 0xFF) << 8;
        l |= (long)(b[readIndex++] & 0xFF) << 16;
        l |= (long)(b[readIndex++] & 0xFF) << 24;
        l |= (long)(b[readIndex++] & 0xFF) << 32;
        l |= (long)(b[readIndex++] & 0xFF) << 40;
        l |= (long)(b[readIndex++] & 0xFF) << 48;
        l |= (long)(b[readIndex++] & 0xFF) << 56;
        return l;
    }

    public String readStringWithNull(){
        return new String(readBytesWithNull());
    }

    public String readStringWithLength(int pos) {
        return new String(readBytesWithLength(pos));
    }

    /**
     * 读取一个字节，获取值的类型
     * @return
     */
    public byte readByte(){
        return buffer[readIndex++];
    }

    /**
     * 读取当前位置指定长度
     * @param length
     * @return
     */
    public byte[] readBytes(int length){
        final byte[] b = this.buffer;
        byte[] res = new byte[length];
        System.arraycopy(b, readIndex, res, 0, length);
        readIndex += length;
        return res;
    }

    /**
     * 读取指定位置指定长度
     * @param pos
     * @param length
     * @return
     */
    public byte[] readBytes(int pos, int length){
        final byte[] b = this.buffer;
        byte[] res = new byte[length];
        System.arraycopy(b, pos, res, 0, length);
        return res;
    }

    /**
     * 这种是不定长的形式，先读取一个int,然后读取length个字节
     * @param pos
     * @return
     */
    public byte[] readBytesWithLength(int pos){
        final byte[] b = this.buffer;
        if(length - pos < 4) return null;
        int len = readIntPos(pos);
        pos += 4;
        if(length - pos < len){
            return null;
        }
        byte[] res = new byte[len];
        System.arraycopy(b, pos, res, 0, len);
        return res;
    }

    /**
     * 读取带空值的字节数组
     * 字节串后面有一个0
     * @return
     */
    public byte[] readBytesWithNull(){
        final byte[] b = this.buffer;
        if(readIndex >= length) return null;
        int offset = -1;
        for(int i = readIndex; i < length; i++){
            if(b[i] == 0){
                offset = i;
                break;
            }
        }
        switch (offset){
            case -1://没有找到结束符,直接返回剩余的字节数组
                byte[] ab1 = new byte[length - readIndex];
                System.arraycopy(b, readIndex, ab1, 0, length - readIndex);
                readIndex = length;
                return ab1;
            case 0://
                readIndex ++;
                return null;
            default:
                byte[] ab2 = new byte[offset - readIndex];
                System.arraycopy(b, readIndex, ab2, 0, offset - readIndex);
                readIndex = offset + 1;
                return ab2;
        }
    }

    public void writeWithLength(byte[] src){
        writeInt(src.length);
        writeBytes(src);
    }
    public void writeStringLength(String s){
        writeWithLength(s.getBytes());
    }

    /**
     * 剩余长度
     * @return
     */
    public int remaining(){
        return length - readIndex;
    }

    public void clean(){
        readIndex = 0;
        writeIndex = 0;
        buffer = new byte[buffer.length];
    }
    public int getLength(){
        return length;
    }

}
