package com.demo.lesson10;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TimerDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 为了展现方便先设置为1,后续降到多并行度的watermark再开启多并行度
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. map做类型转换
        SingleOutputStreamOperator<Tuple3<String,Double,Long>> map = text.map(new Tuple3MapFunction());
        // 4. 定义乱序watermark以及TimestampAssigner
        WatermarkStrategy<Tuple3<String,Double,Long>> watermarkStrategy = WatermarkStrategy
                // 设置乱序watermark，需要指定水位线延迟时间，设置2秒
                .<Tuple3<String,Double,Long>>forMonotonousTimestamps()
                // 设置事件时间处理器
                .withTimestampAssigner((element, recordTimestamp) ->{
                    return element.f2 * 1000L;
                } );
        SingleOutputStreamOperator<Tuple3<String,Double,Long>> mapWithWatermark = map.assignTimestampsAndWatermarks(watermarkStrategy);
        // 5. 做keyBy
        KeyedStream<Tuple3<String,Double,Long>, String> kyStream = mapWithWatermark.keyBy(new KeySelectorFunction());
        SingleOutputStreamOperator<String> process = kyStream.process(new KeyedProcessFunction<>() {
            @Override
            public void processElement(Tuple3<String, Double, Long> value, KeyedProcessFunction<String, Tuple3<String, Double, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                TimerService timerService = ctx.timerService();
                // 获得事件时间的watermark
                long currentWatermark = timerService.currentWatermark();
                System.out.println("当前事件时间为=" + currentWatermark);
                // 注册5秒事件时间的定时器
                timerService.registerEventTimeTimer(5000L);
                System.out.println("key="+value.f0+"注册一个5秒的事件时间定时器");

                /**
                 * 处理时间的定时器
                 */
//                timerService.currentProcessingTime();//当前处理时间
//                timerService.registerProcessingTimeTimer(5000L);//注册一个处理时间定时器
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple3<String, Double, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                // 触发输出一个数据到下一个算子
                System.out.println("触发器触发，时间="+timestamp);
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
