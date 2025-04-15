package com.demo.lesson11.keyby;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ValueState2Demo {

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
        // 6. 使用map做数据累加
        SingleOutputStreamOperator<String> process = kyStream.map(new CustomMapFunction());
        // 7. 打印
        process.print();
        // 执行
        env.execute();
    }

    public static class CustomMapFunction implements MapFunction<Tuple3<String,Double,Long>, String>, CheckpointedFunction {

        // 1）定义ValueState值
        ValueState<Double> currentValue;

        /**
         * 初始化算子状态
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 2）初始化ValueState值
            System.out.println("initializeState...");
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("currentValue", Types.DOUBLE);
            currentValue = context.getKeyedStateStore().getState(descriptor);
        }

        @Override
        public String map(Tuple3<String,Double,Long> value) throws Exception {
            double curValue = 0l;
            // 3）获取ValueState值
            if(currentValue.value()==null){
                curValue = value.f1;
            }else{
                curValue = currentValue.value() + value.f1;
            }
            // 4）更新ValueState值
            currentValue.update(curValue);
            value.f1 = curValue;
            return value.toString();
        }

        /**
         * 在检查点触发时的操作
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState...");
        }

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
