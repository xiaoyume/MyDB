package org.mydb.store.page;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author xiaoy
 * @version 1.0
 * @description: TODO
 * @date 2024/1/12 10:45
 */
public class StoreTest {
    @Test
    public void storeTest() throws FileNotFoundException {
        String path = "D:\\test";
        writeToFile(path, "jjjjjjjjjjjjj");
        readFromFile(path);

    }

    // 写入文件
    private static void writeToFile(String filePath, String content) {
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "rw")) {
            // 将文件指针移到文件末尾
            raf.seek(raf.length());

            // 写入内容
            raf.writeUTF(content);

            System.out.println("Write to file: " + content);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 读取文件
    private static void readFromFile(String filePath) {
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            // 将文件指针移到文件开头
            raf.seek(0);

            // 读取内容
            String content = raf.readUTF();

            System.out.println("Read from file: " + content);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
