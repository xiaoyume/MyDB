package org.mydb.meta.value;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 设定一个boolean类型占用两个字节，type + data
 * @date 2023/11/28 10:22
 */
public class ValueBoolean extends Value{

    private boolean b;

    public ValueBoolean() {
    }
    public ValueBoolean(boolean b) {
        this.b = b;
    }
    /**一个boolean用两个字节表示
     * [type][data]
     *
     * @return
     */
    @Override
    public int getLength() {
        return 1 + 1;
    }

    @Override
    public byte getType() {
        return BOOLEAN;
    }

    /**
     *[type][data]
     *
     */
    @Override
    public byte[] getBytes() {
        byte[] res = new byte[2];
        res[0] = BOOLEAN;
        if(b){
            res[1] = 1;
        }else{
            res[1] = 0;
        }
        return res;
    }

    @Override
    public void read(byte[] bytes) {
        if(bytes[0] == 0){
            b = false;
        }else {
            b = true;
        }
    }

    /**比较，0:相等，1:大于，-1:小于
     * @param value
     * @return
     */
    @Override
    public int compare(Value value) {
        ((ValueBoolean) value).getBoolean()
        if(b){
            if(toCompare){
                return 0;
            }else{
                return 1;
            }
        }else{
            if(toCompare){
                return -1;
            }else{
                return 0;
            }
        }
    }

    @Override
    public String toString() {
        if(b){
            return "true";
        }else {
            return "false";
        }
    }

    public boolean getBoolean(){
        return b;
    }
    public ValueBoolean setBoolean(boolean b){
        this.b = b;
        return this;
    }
}
