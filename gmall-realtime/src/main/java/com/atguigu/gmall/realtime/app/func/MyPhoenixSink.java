package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DimUtil;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

import static com.atguigu.gmall.realtime.util.DruidDSUtil.druidDataSource;

public class MyPhoenixSink extends RichSinkFunction<JSONObject> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        // 获取目标表表名
        String sinkTable = jsonObj.getString("sinkTable");
        // 获取操作类型
        String type = jsonObj.getString("type");
        // 获取 id 字段的值
        String id = jsonObj.getString("id");

        // 清除 JSON 对象中的 sinkTable 字段和 type 字段
        // 以便可将该对象直接用于 HBase 表的数据写入
        jsonObj.remove("sinkTable");
        jsonObj.remove("type");

        // 获取字段名
        Set<String> columns = jsonObj.keySet();
        // 获取字段对应的值
        Collection<Object> values = jsonObj.values();
        // 拼接字段名
        String columnStr = StringUtils.join(columns, ",");
        // 拼接字段值
        String valueStr = StringUtils.join(values, "','");
        // 拼接插入语句
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA
                + "." + sinkTable + "(" +
                columnStr + ") values ('" + valueStr + "')";

        // 获取连接对象
        DruidPooledConnection conn = null;
        try {
            conn = druidDataSource.getConnection();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.out.println("从 Druid 连接池获取连接对象异常");
        }
        PhoenixUtil.executeSQL(sql, conn);

        // 如果操作类型为 update，则清除 redis 中的缓存信息
        if ("update".equals(type)) {
            DimUtil.deleteCached(sinkTable, id);
        }
    }
}
