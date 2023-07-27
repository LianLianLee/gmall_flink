package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

public class BaseDBBroadcastFunc extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    //定义一个HashMap用于存放预加载的配置信息
    private HashMap<String, TableProcess> configMap;

    private MapStateDescriptor<String, TableProcess> tableProcessStateDes;

    public BaseDBBroadcastFunc(MapStateDescriptor<String, TableProcess> tableProcessStateDes) {
        this.tableProcessStateDes = tableProcessStateDes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        configMap = new HashMap<>();
        Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall-220623-config", "root", "123456");

        List<TableProcess> tableProcessList = JdbcUtil.queryList(connection,
                "select * from table_process where sink_type='dwd'",
                TableProcess.class,
                true);

        //将预读取的配置信息存放进Map中
        for (TableProcess tableProcess : tableProcessList) {
            String key = tableProcess.getSourceTable() + "-" + tableProcess.getSourceType();
            configMap.put(key, tableProcess);
        }
    }

    //value:{"before":null,"after":{"source_table":"base_category3","sink_table":"dim_base_category3","sink_columns":"id,name,category2_id","sink_pk":"id","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1669162876406,"snapshot":"false","db":"gmall-220623-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1669162876406,"transaction":null}
    @Override
    public void processBroadcastElement(String value, Context context, Collector<JSONObject> collector) throws Exception {

        //0.获取广播状态数据
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(tableProcessStateDes);

        //1.解析数据为TableProcess对象
        //如果为删除操作,则删除状态以及Map中的配置信息
        JSONObject jsonObject = JSONObject.parseObject(value);
        if ("d".equals(jsonObject.getString("op"))) {
            TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("before"), TableProcess.class);
            String key = tableProcess.getSourceTable() + "-" + tableProcess.getSourceType();
            configMap.remove(key);
            broadcastState.remove(key);
        } else {

            //2.放入状态广播出去
            TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("after"), TableProcess.class);
            String key = tableProcess.getSourceTable() + "-" + tableProcess.getSourceType();
            broadcastState.put(key, tableProcess);
        }
    }

    //jsonObject:{"database":"gmall-220623-flink","table":"comment_info","type":"insert","ts":1669162958,"xid":1111,"xoffset":13941,"data":{"id":1595211185799847960,"user_id":119,"nick_name":null,"head_img":null,"sku_id":31,"spu_id":10,"order_id":987,"appraise":"1204","comment_txt":"评论内容：48384811984748167197482849234338563286217912223261","create_time":"2022-08-02 08:22:38","operate_time":null}}
    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        //从广播状态中提取对应的配置信息
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(tableProcessStateDes);
        String key = jsonObject.getString("table") + "-" + jsonObject.getString("type");

        //根据存配置信息存在与否,过滤数据
        if (broadcastState.contains(key) || configMap.containsKey(key)) {

            //提取配置信息
            TableProcess tableProcess = broadcastState.get(key);
            if (tableProcess == null) {
                tableProcess = configMap.get(key);
            }

            //过滤字段
            JSONObject data = jsonObject.getJSONObject("data");
            filterSinkColumns(data, tableProcess.getSinkColumns());

            //补充SinkTable字段并写出数据,未来的主题名
            jsonObject.put("sink_table", tableProcess.getSinkTable());

            //输出数据
            collector.collect(jsonObject);

        } else {
            System.out.println("组合Key：" + key + "不存在！");
        }

    }


    // 过滤json数据  只保留sinkColumns中包含的字段
    private static void filterSinkColumns(JSONObject data, String sinkColumns) {

        // 如果sinkColumns   -> activity_user_info_id,id,info_info
        // data   -> user_info_id
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        List<String> strings = Arrays.asList(sinkColumns.split(","));

        // 使用迭代器删除
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            String key = iterator.next().getKey();
            if (!strings.contains(key)) {
                iterator.remove();
            }
        }
//        entries.removeIf(entry -> !strings.contains(entry.getKey()));

    }

}
