package org.mydb.store.fs;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2023/11/30 20:55
 */
public class FileUtils {
    /**
     * 判断给定的文件是否存在
     * @param fileName 文件名
     * @return 存在返回true，否则返回false
     */
    public static boolean exists(String fileName){
        return new File(fileName).exists();
    }

    /**获取文件读写通道
     * @param fileName
     * @return
     */
    public static FileChannel open(String fileName){
        try {
            RandomAccessFile file = new RandomAccessFile(fileName, "rw");
            return file.getChannel();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeFile(FileChannel channel){
        try {
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void readFully(FileChannel channel, ByteBuffer buffer, long pos) throws IOException {
        if(channel.position() != pos){
            channel.position(pos);
        }
        do {
            int r = channel.read(buffer);
            //r是读取的字节数，如果小于0，说明已经到文件尾部，抛出异常
            if(r < 0){
                throw new EOFException();
            }
        }while (buffer.remaining() > 0);

    }

    public static void writeFully(FileChannel channel, ByteBuffer buffer, long pos) throws IOException {
        if(channel.position()!= pos){
            channel.position(pos);
        }
        do {
            channel.write(buffer);
        }while (buffer.remaining() > 0);
    }

}
