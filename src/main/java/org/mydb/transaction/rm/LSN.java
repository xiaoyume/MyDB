package org.mydb.transaction.rm;

/**
 * @author xiaoy
 * @version 1.0
 * @description: Log Sequence Number 日志序列号
 * @date 2024/2/16 20:20
 */
public class LSN implements Comparable{
    //记录在文件中的相对字节地址
    private Integer rba;

    @Override
    public int compareTo(Object o) {
        LSN other = (LSN) o;
        if(this == other){
            return 0;
        }
        return rba.compareTo(((LSN)o).rba);
    }
    public Integer getRba(){
        return rba;
    }
    public LSN setRba(Integer rba){
        this.rba = rba;
        return this;
    }
}
