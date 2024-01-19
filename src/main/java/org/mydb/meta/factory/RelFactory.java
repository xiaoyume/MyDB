package org.mydb.meta.factory;

import org.mydb.config.SystemConfig;
import org.mydb.meta.Relation;

/**
 * @author xiaoy
 * @version 1.0
 * @description: 单例模式的表工厂，负责创建表对象
 * @date 2023/12/25 19:43
 */
public class RelFactory {
    private static RelFactory relFactory;
    static {
        relFactory = new RelFactory();
    }
    public static RelFactory getInstance(){
        return relFactory;
    }
    public Relation newRelation(String tableName){
        Relation relation = new Relation();
        relation.setRelPath(SystemConfig.RELATION_FILE_PRE_FIX + tableName + ".txt");
        relation.setMetaPath(SystemConfig.RELATION_FILE_PRE_FIX + tableName + "_meta.txt");
        return relation;
    }
}
