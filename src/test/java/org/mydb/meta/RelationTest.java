package org.mydb.meta;

import org.junit.Test;
import org.mydb.access.SeqCursor;
import org.mydb.meta.factory.RelFactory;
import org.mydb.meta.value.Value;
import org.mydb.meta.value.ValueLong;
import org.mydb.meta.value.ValueString;
import org.mydb.store.item.Item;

public class RelationTest {
    @Test
    public void testRelation() {

        Relation relation = RelFactory.getInstance().newRelation("test1");
        relation.open();
        relation.setTupleDesc(getTupleDesc());
        relation.writeMeta();
        relation.writePageOffInfo();
        relation.close();

        //再读
        Relation relation2 = RelFactory.getInstance().newRelation("test1");
        relation2.open();
        relation2.loadFromDisk();

        Value[] values = new Value[2];
        values[0] = new ValueLong(1);
        values[1] = new ValueString("testjjjjjj");
        Item item = new Item(new Tuple(values));
        for (int i = 0; i < 10; i++) {
            relation2.insert(item);
        }
        relation2.flushToDisk();

        SeqCursor seqScanner = new SeqCursor(relation2);
        while(true){
            Tuple tuple = seqScanner.getNext();
            if(tuple == null){
                break;
            }else{
                System.out.println(tuple);
            }
        }

    }


    public TupleDesc getTupleDesc() {
        //attribute就是表中的属性项
        Attribute[] attributes = new Attribute[2];
        Attribute attrId = new Attribute();
        attrId.setName("id");
        attrId.setType(Value.LONG);
        attrId.setIndex(0);
        attrId.setComment("primary key");

        Attribute attrName = new Attribute();
        attrName.setName("name");
        attrName.setType(Value.STRING);
        attrName.setIndex(1);
        attrName.setComment("name key");

        attributes[0] = attrId;
        attributes[1] = attrName;
        return new TupleDesc(attributes);
    }
}