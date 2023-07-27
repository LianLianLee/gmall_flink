package com.atguigu.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    private static ThreadPoolExecutor poolExecutor;

    static {
        System.out.println("---创建线程池---");
        poolExecutor = new ThreadPoolExecutor(
                4, 20, 60 * 5,
                TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
    }

    public static ThreadPoolExecutor getInstance() {
        return poolExecutor;
    }
}
（10）异步 IO 函数 DimAsyncFunction
        package com.atguigu.gmall.realtime.app.func;

        import com.alibaba.fastjson.JSONObject;
        import com.atguigu.gmall.realtime.util.DimUtil;
        import com.atguigu.gmall.realtime.util.ThreadPoolUtil;
        import org.apache.flink.configuration.Configuration;
        import org.apache.flink.streaming.api.functions.async.ResultFuture;
        import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

        import java.util.Collections;
        import java.util.concurrent.ExecutorService;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    // 表名
    private String tableName;

    // 线程池操作对象
    private ExecutorService executorService;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //从线程池中获取线程，发送异步请求
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // 1. 根据流中的对象获取维度的主键
                            String key = getKey(obj);

                            // 2. 根据维度的主键获取维度对象

                            JSONObject dimJsonObj = null;
                            try {
                                dimJsonObj = DimUtil.getDimInfo(tableName, key);
                            } catch (Exception e) {
                                System.out.println("维度数据异步查询异常");
                                e.printStackTrace();
                            }

                            // 3. 将查询出来的维度信息 补充到流中的对象属性上
                            if (dimJsonObj != null) {
                                join(obj, dimJsonObj);
                            }
                            resultFuture.complete(Collections.singleton(obj));
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("异步维度关联发生异常了");
                        }
                    }
                }
        );
    }
}
