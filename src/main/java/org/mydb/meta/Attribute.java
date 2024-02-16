package org.mydb.meta;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 属性
 * @date 2023/11/27 19:52
 */
public class Attribute {
    private String name;
    private int type;
    private int index;
    private String comment;

    public Attribute() {
    }

    public Attribute(String name, int type, int index, String comment) {
        this.name = name;
        this.type = type;
        this.index = index;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public Attribute setName(String name) {
        this.name = name;
        return this;
    }

    public int getType() {
        return type;
    }

    public Attribute setType(int type) {
        this.type = type;
        return this;
    }

    public int getIndex() {
        return index;
    }

    public Attribute setIndex(int index) {
        this.index = index;
        return this;
    }

    public String getComment() {
        return comment;
    }

    public Attribute setComment(String comment) {
        this.comment = comment;
        return this;
    }
}
