package com.demo.lesson11.ttl;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TTLDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. map做类型转换
        SingleOutputStreamOperator<Tuple3<String,Double,Long>> map = text.map(new Tuple3MapFunction());
        // 4. 定义单调递增watermark以及TimestampAssigner
        WatermarkStrategy<Tuple3<String,Double,Long>> watermarkStrategy = WatermarkStrategy
                // 设置单调递增
                .<Tuple3<String,Double,Long>>forMonotonousTimestamps()
                // 设置事件时间处理器
                .withTimestampAssigner((element, recordTimestamp) ->{
                    return element.f2 * 1000L;
                } );
        SingleOutputStreamOperator<Tuple3<String,Double,Long>> mapWithWatermark = map.assignTimestampsAndWatermarks(watermarkStrategy);
        // 5. 做keyBy
        KeyedStream<Tuple3<String,Double,Long>, String> kyStream = mapWithWatermark.keyBy(new KeySelectorFunction());
        SingleOutputStreamOperator<String> process = kyStream.process(new KeyedProcessFunction<>() {

            // 1）定义ValueState值
            ValueState<Tuple3<String, Double, Long>> currentValue;

            @Override
            public void open(OpenContext openContext) throws Exception {
                super.open(openContext);
                // 2) 设置TTL策略
                StateTtlConfig stateTtlConfig = StateTtlConfig
                        // 设置10秒钟过期
                        .newBuilder(Duration.ofSeconds(10))
                        // 设置什么条件下更新过期时间，支持3种方式：创建和写入（OnCreateAndWrite）、读取和写入（OnReadAndWrite）、都不更新（Disabled）
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        // 设置是否返回过期未清除数据（因为Flink清除数据机制是通过标识某个数据过期，另外有一个线程定时清除数据，这样就会导致有些数据过期，但是还未清除）
                        // 支持2种方式：不返回过期未清除数据（NeverReturnExpired）、返回过期未清除数据（ReturnExpiredIfNotCleanedUp）
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();
                // 3）初始化ValueState值
                ValueStateDescriptor<Tuple3<String, Double, Long>> descriptor = new ValueStateDescriptor<>("currentValue", Types.TUPLE(Types.STRING, Types.DOUBLE, Types.LONG));
                descriptor.enableTimeToLive(stateTtlConfig);
                currentValue = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(Tuple3<String, Double, Long> value, KeyedProcessFunction<String, Tuple3<String, Double, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                // 4）获取ValueState值
                Tuple3<String, Double, Long> curValue = currentValue.value();
                if(curValue==null){
                    curValue = value;
                }else{
                    curValue.f1 = curValue.f1 + value.f1;
                }
                // 5）更新ValueState值
                currentValue.update(curValue);
                out.collect(curValue.toString());
            }

        });
        // 6. 打印
        process.print();
        // 执行
        env.execute();
    }


    public static class Tuple3MapFunction implements MapFunction<String, Tuple3<String,Double,Long>> {

        @Override
        public Tuple3<String, Double, Long> map(String value) throws Exception {
            String[] values = value.split(",");
            String value1 = values[0];
            double value2 = Double.parseDouble("0");
            long value3 = 0;
            if(values.length >= 2){
                try {
                    value2 = Double.parseDouble(values[1]);
                }catch (Exception e){
                    value2 = Double.parseDouble("0");
                }
            }
            if(values.length >= 3){
                try {
                    value3 = Long.parseLong(values[2]);
                }catch (Exception ignored){
                }
            }
            return new Tuple3<>(value1,value2,value3);
        }
    }

    public static class KeySelectorFunction implements KeySelector<Tuple3<String,Double,Long>, String> {

        @Override
        public String getKey(Tuple3<String,Double,Long> value) throws Exception {
            // 返回第一个值，作为keyBy的分类
            return value.f0;
        }

    }
}
