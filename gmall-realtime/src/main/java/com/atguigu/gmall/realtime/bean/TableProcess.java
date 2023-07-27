package com.atguigu.gmall.realtime.bean;

import lombok.Data;

@Data
public class TableProcess {
    // 来源表
    String sourceTable;

    // 来源操作类型
    String sourceType;

    // 输出表
    String sinkTable;

    // 输出类型 dwd | dim
    String sinkType;

    // 输出字段
    String sinkColumns;

    // 主键字段
    String sinkPk;

    // 建表扩展
    String sinkExtend;
}
