package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

// BroadcastProcessFunction是Flink中的一个算子，用于广播流处理。它可以将广播流和主流连接起来，然后对主流进行处理。123
public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> tableConfigDescriptor;

    // 定义预加载配置对象
    HashMap<String, String> configMap = new HashMap<>();

    public MyBroadcastFunction(MapStateDescriptor<String, TableProcess> tableConfigDescriptor) {
        this.tableConfigDescriptor = tableConfigDescriptor;
    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        Set<Map.Entry<String, Object>> dataEntries = data.entrySet();
        dataEntries.removeIf(r -> !sinkColumns.contains(r.getKey()));
    }

    /**
     * Phoenix 建表函数
     *
     * @param sinkTable 目标表名  eg. test
     * @param sinkColumns 目标表字段  eg. id,name,sex
     * @param sinkPk 目标表主键  eg. id
     * @param sinkExtend 目标表建表扩展字段  eg. ""
     *                   eg. create table if not exists mydb.test(id varchar primary key, name varchar, sex varchar)...
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        // 封装建表 SQL
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists " + GmallConfig.HBASE_SCHEMA
                + "." + sinkTable + "(\n");
        String[] columnArr = sinkColumns.split(",");
        // 为主键及扩展字段赋默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        // 遍历添加字段信息
        for (int i = 0; i < columnArr.length; i++) {
            sql.append(columnArr[i] + " varchar");
            // 判断当前字段是否为主键
            if (sinkPk.equals(columnArr[i])) {
                sql.append(" primary key");
            }
            // 如果当前字段不是最后一个字段，则追加","
            if (i < columnArr.length - 1) {
                sql.append(",\n");
            }
        }
        sql.append(")");
        sql.append(sinkExtend);
        String createStatement = sql.toString();

        PhoenixUtil.executeSQL(createStatement);
    }


    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext readOnlyContext, Collector<JSONObject> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> tableConfigState = readOnlyContext.getBroadcastState(tableConfigDescriptor);

        // 获取配置信息
        String sourceTable = jsonObj.getString("table");
        TableProcess tableConfig = tableConfigState.get(sourceTable);

// 状态中没有获取到配置信息时通过 configMap 获取
        if (tableConfig == null) {
            tableConfig = JSON.parseObject(configMap.get(sourceTable), TableProcess.class);
        }

        if (tableConfig != null) {
            JSONObject data = jsonObj.getJSONObject("data");
            // 获取操作类型
            String type = jsonObj.getString("type");
            String sinkTable = tableConfig.getSinkTable();

            // 根据 sinkColumns 过滤数据
            String sinkColumns = tableConfig.getSinkColumns();
            filterColumns(data, sinkColumns);

            // 将目标表名加入到主流数据中
            data.put("sinkTable", sinkTable);

            // 将操作类型加入到 JSONObject 中
            data.put("type", type);

            out.collect(data);
        }
    }

//    @Override
//    public void processElement(JSONObject jsonObj, ReadOnlyContext readOnlyContext, Collector<JSONObject> out) throws Exception {
//
//        ReadOnlyBroadcastState<String, TableProcess> tableConfigState = readOnlyContext.getBroadcastState(tableConfigDescriptor);
//
//        // 获取配置信息
//        String sourceTable = jsonObj.getString("table");
//        TableProcess tableConfig = tableConfigState.get(sourceTable);
//        if (tableConfig != null) {
//            JSONObject data = jsonObj.getJSONObject("data");
//            // 获取操作类型
//            String type = jsonObj.getString("type");
//            String sinkTable = tableConfig.getSinkTable();
//
//            // 根据 sinkColumns 过滤数据
//            String sinkColumns = tableConfig.getSinkColumns();
//            filterColumns(data, sinkColumns);
//
//            // 将目标表名加入到主流数据中
//            data.put("sinkTable", sinkTable);
//
//            // 将操作类型加入到 JSONObject 中
//            data.put("type", type);
//
//            out.collect(data);
//        }
//    }

    @Override
    public void open(Configuration parameter) throws Exception {
        super.open(parameter);

        // 预加载配置信息
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_config?" +
                "user=root&password=123456&useUnicode=true&" +
                "characterEncoding=utf8&serverTimeZone=Asia/Shanghai&useSSL=false");

        String sql = "select * from gmall_config.table_process where sink_type='dim'";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        ResultSet rs = preparedStatement.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            JSONObject jsonValue = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                String columnValue = rs.getString(i);
                jsonValue.put(columnName, columnValue);
            }

            String key = jsonValue.getString("source_table");
            configMap.put(key, jsonValue.toJSONString());
        }

        rs.close();
        preparedStatement.close();
        conn.close();
    }

    @Override
    public void processBroadcastElement(String jsonStr, Context context, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObj = JSON.parseObject(jsonStr);

        BroadcastState<String, TableProcess> tableConfigState = context.getBroadcastState(tableConfigDescriptor);

        String op = jsonObj.getString("op");

        // 若操作类型是删除，则清除广播状态中的对应值
        if ("d".equals(op)) {
            TableProcess before = jsonObj.getObject("before", TableProcess.class);
            String sinkTable = before.getSinkTable();
            // 只有输出类型为 "dim" 时才进行后续操作
            if ("dim".equals(sinkTable)) {
                String sourceTable = before.getSourceTable();
                tableConfigState.remove(sourceTable);

// 清除 configMap 中的对应值
                configMap.remove(sourceTable);
            }
            // 操作类型为更新或插入，利用 Map key 的唯一性直接写入，状态中保留的是插入或更新的值
        } else {
            TableProcess config = jsonObj.getObject("after", TableProcess.class);
            String sinkType = config.getSinkType();
            // 只有输出类型为 "dim" 时才进行后续操作
            if("dim".equals(sinkType)) {
                String sourceTable = config.getSourceTable();
                String sinkTable = config.getSinkTable();
                String sinkColumns = config.getSinkColumns();
                String sinkPk = config.getSinkPk();
                String sinkExtend = config.getSinkExtend();

                tableConfigState.put(sourceTable , config);

// 将修改同步到 configMap
                configMap.put(sourceTable, JSON.toJSONString(config));

                checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
            }
        }
    }




}
