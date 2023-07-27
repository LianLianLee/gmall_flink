package com.atguigu.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.BaseDBBroadcastFunc;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 读取业务主流
        String topic = "topic_db";
        String groupId = "base_db_app";
        DataStreamSource<String> gmallDS = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 4. 主流 ETL
        SingleOutputStreamOperator<String> filterDS = gmallDS.filter(
                jsonStr ->
                {
                    try {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        jsonObj.getJSONObject("data");
                        return !jsonObj.getString("type").equals("bootstrap-start")
                                && !jsonObj.getString("type").equals("bootstrap-complete")
                                && !jsonObj.getString("type").equals("bootstrap-insert");
                    } catch (Exception exception) {
                        exception.printStackTrace();
                        return false;
                    }
                });

        // TODO 5. 主流数据结构转换
        SingleOutputStreamOperator<JSONObject> jsonDS = filterDS.map(JSON::parseObject);

        // TODO 6. FlinkCDC 读取配置流并广播流
        // 6.1 FlinkCDC 读取配置表信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config") // set captured database
                .tableList("gmall_config.table_process") // set captured table
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();

        // 6.2 封装为流
        DataStreamSource<String> mysqlDSSource = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "base-db-mysql-source")
                .setParallelism(1);

        // 6.3 广播配置流
        MapStateDescriptor<String, String> tableConfigDescriptor =
                new MapStateDescriptor<String, String>("dwd-table-process-state", String.class, String.class);
        BroadcastStream<String> broadcastDS = mysqlDSSource.broadcast(tableConfigDescriptor);

        // TODO 7. 连接流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonDS.connect(broadcastDS);

        // TODO 8. 处理主流数据
        SingleOutputStreamOperator<JSONObject> dimDS = connectedStream.process(
                new BaseDBBroadcastFunc(tableConfigDescriptor)
        );

        // TODO 9. 将数据写入 Kafka
        dimDS.addSink(KafkaUtil.<JSONObject>getProducerBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                        String topic = jsonObj.getString("sinkTable");
                        // sinkTable 字段不需要写出，清除
                        jsonObj.remove("sinkTable");

                        return new ProducerRecord<byte[], byte[]>(topic, jsonObj.toJSONString().getBytes());
                    }
                }
        ));

        env.execute();
    }
}
