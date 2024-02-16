package org.mydb.index.bp;

import org.mydb.meta.Tuple;

/**
 * @author xiaoy
 * @version 1.0
 * @description: get请求的返回值
 * @date 2024/2/16 15:59
 */
public class GetRes {
    private BPNode bpNode;
    private Tuple tuple;
    public GetRes(BPNode bpNode, Tuple tuple) {
        this.bpNode = bpNode;
        this.tuple = tuple;
    }
    public BPNode getBpNode() {
        return bpNode;
    }
    public GetRes setBpNode(BPNode bpNode) {
        this.bpNode = bpNode;
        return this;
    }
    public Tuple getTuple() {
        return tuple;
    }
    public GetRes setTuple(Tuple tuple) {
        this.tuple = tuple;
        return this;
    }
}
