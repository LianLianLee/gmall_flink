package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.TradeProvinceOrderWindow;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class DwsTradeProvinceOrderWindow {
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
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka dwd_trade_order_detail 主题读取业务数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_province_order_window";

        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 过滤 null 数据
        SingleOutputStreamOperator<String> filteredStream = source.filter(Objects::nonNull);

        // TODO 5. 按照 order_detail_id 分组
        KeyedStream<String, String> keyedStream = filteredStream.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String jsonStr) throws Exception {
                JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                return jsonObj.getString("id");
            }
        });

        // TODO 6. 去重并转换数据结构
        SingleOutputStreamOperator<TradeProvinceOrderWindow> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, String, TradeProvinceOrderWindow>() {

                    private ValueState<JSONObject> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("last_value_state", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<TradeProvinceOrderWindow> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject lastValue = lastValueState.value();
                        if (lastValue != null) {
                            String provinceId = lastValue.getString("province_id");
                            String orderId = lastValue.getString("order_id");
                            BigDecimal orderAmount = new BigDecimal(
                                    "-" + lastValue.getString("split_total_amount"));
                            Long ts = lastValue.getLong("ts") * 1000L;

                            out.collect(TradeProvinceOrderWindow.builder()
                                    .provinceId(provinceId)
                                    .orderId(orderId)
                                    .orderAmount(orderAmount)
                                    .ts(ts)
                                    .build());
                        }
                        lastValueState.update(jsonObj);
                        String provinceId = jsonObj.getString("province_id");
                        String orderId = jsonObj.getString("order_id");
                        BigDecimal orderAmount = new BigDecimal(jsonObj.getString("split_total_amount"));
                        Long ts = jsonObj.getLong("ts") * 1000L;

                        out.collect(TradeProvinceOrderWindow.builder()
                                .provinceId(provinceId)
                                .orderId(orderId)
                                .orderAmount(orderAmount)
                                .ts(ts)
                                .build());
                    }
                }
        );

        // TODO 7. 按照 order_id 分组
        KeyedStream<TradeProvinceOrderWindow, String> orderIdKeyedStream = processedStream.keyBy(TradeProvinceOrderWindow::getOrderId);

        // TODO 8. 统计订单数
        SingleOutputStreamOperator<TradeProvinceOrderWindow> withOrderCountStream = orderIdKeyedStream.process(
                new KeyedProcessFunction<String, TradeProvinceOrderWindow, TradeProvinceOrderWindow>() {

                    ValueState<String> lastOrderIdState;

                    @Override
                    public void open(Configuration paramters) throws Exception {
                        super.open(paramters);
                        ValueStateDescriptor<String> lastOrderIdStateDescriptor =
                                new ValueStateDescriptor<>("province_last_order_id_state", String.class);
                        lastOrderIdStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.seconds(10L)).build()
                        );
                        lastOrderIdState = getRuntimeContext().getState(lastOrderIdStateDescriptor);
                    }

                    @Override
                    public void processElement(TradeProvinceOrderWindow javaBean, Context context, Collector<TradeProvinceOrderWindow> out) throws Exception {
                        String orderId = javaBean.getOrderId();
                        String lastOrderId = lastOrderIdState.value();
                        if (lastOrderId == null) {
                            javaBean.setOrderCount(1L);
                            out.collect(javaBean);
                            lastOrderIdState.update(orderId);
                        } else {
                            javaBean.setOrderCount(0L);
                            out.collect(javaBean);
                        }
                    }
                }
        );

        // TODO 9. 设置水位线
        SingleOutputStreamOperator<TradeProvinceOrderWindow> withWatermarkStream = withOrderCountStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeProvinceOrderWindow>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
                                    @Override
                                    public long extractTimestamp(TradeProvinceOrderWindow javaBean, long recordTimestamp) {
                                        return javaBean.getTs();
                                    }
                                }
                        )
        );

        // TODO 10. 按照省份 ID 分组
        KeyedStream<TradeProvinceOrderWindow, String> keyedByProIdStream =
                withWatermarkStream.keyBy(TradeProvinceOrderWindow::getProvinceId);

        // TODO 11. 开窗
        WindowedStream<TradeProvinceOrderWindow, String, TimeWindow> windowDS = keyedByProIdStream
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(5L));

        // TODO 12. 聚合计算
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reducedStream = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<TradeProvinceOrderWindow> elements, Collector<TradeProvinceOrderWindow> out) throws Exception {
                        for (TradeProvinceOrderWindow element : elements) {
                            String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                            String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 13. 关联省份信息
        SingleOutputStreamOperator<TradeProvinceOrderWindow> fullInfoStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<TradeProvinceOrderWindow>("dim_base_province".toUpperCase()) {

                    @Override
                    public void join(TradeProvinceOrderWindow javaBean, JSONObject jsonObj) throws Exception {
                        String provinceName = jsonObj.getString("name".toUpperCase());
                        javaBean.setProvinceName(provinceName);
                    }

                    @Override
                    public String getKey(TradeProvinceOrderWindow javaBean) {
                        return javaBean.getProvinceId();
                    }
                },
                60 * 50, TimeUnit.SECONDS
        );

        // TODO 14. 写入到 OLAP 数据库
        SinkFunction<TradeProvinceOrderWindow> jdbcSink = ClickHouseUtil.<TradeProvinceOrderWindow>getJdbcSink(
                "insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"
        );
        fullInfoStream.<TradeProvinceOrderWindow>addSink(jdbcSink);

        env.execute();
    }
}
