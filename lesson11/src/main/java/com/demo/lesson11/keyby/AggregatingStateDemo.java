package com.demo.lesson11.keyby;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AggregatingStateDemo {

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

            // 1）定义AggregatingState值
            AggregatingState<Tuple3<String, Double, Long>, Double> currentValue;

            @Override
            public void open(OpenContext openContext) throws Exception {
                super.open(openContext);
                // 2）初始化AggregatingState值
                AggregatingStateDescriptor<Tuple3<String, Double, Long>, Double, Double> descriptor = new AggregatingStateDescriptor<>(
                        "currentValue",
                        new AggregateFunction<>() {
                            @Override
                            public Double createAccumulator() {
                                return 0.0;
                            }

                            @Override
                            public Double add(Tuple3<String, Double, Long> value, Double accumulator) {
                                accumulator = accumulator + value.f1;
                                return accumulator;
                            }

                            @Override
                            public Double getResult(Double accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Double merge(Double a, Double b) {
                                return 0.0;
                            }
                        },
                        Types.DOUBLE);
                currentValue = getRuntimeContext().getAggregatingState(descriptor);
            }

            @Override
            public void processElement(Tuple3<String, Double, Long> value, KeyedProcessFunction<String, Tuple3<String, Double, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                // 3）更新AggregatingState值
                currentValue.add(value);
                // 4）获取AggregatingState值
                out.collect(currentValue.get().toString());
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
