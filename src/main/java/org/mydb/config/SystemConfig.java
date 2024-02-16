package org.mydb.config;

public interface SystemConfig {
    //默认页大小，4kB
    int DEFAULT_PAGE_SIZE = 4096;

    int DEFAULT_SPECIAL_POINT_LENGTH = 64;
    //关系表存储路径前缀
    String RELATION_FILE_PRE_FIX = "D:/freedom_master/mydb/";
    String FREEDOM_REL_PATH = "D:/freedom_master/mydb/t_freedom.txt";
    String FREEDOM_REL_META_PATH = "D:/freedom_master/mydb/t_freedom_meta";
    //日志文件
    String FREEDOM_LOG_FILE_NAME = "D:/freedom_master/mydb/t_freedom_log";
}
