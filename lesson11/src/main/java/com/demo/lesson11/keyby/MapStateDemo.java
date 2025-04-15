package com.demo.lesson11.keyby;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

public class MapStateDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. map做类型转换
        SingleOutputStreamOperator<Tuple3<String,String,Long>> map = text.map(new Tuple3MapFunction());
        // 4. 定义单调递增watermark以及TimestampAssigner
        WatermarkStrategy<Tuple3<String,String,Long>> watermarkStrategy = WatermarkStrategy
                // 设置单调递增
                .<Tuple3<String,String,Long>>forMonotonousTimestamps()
                // 设置事件时间处理器
                .withTimestampAssigner((element, recordTimestamp) ->{
                    return element.f2 * 1000L;
                } );
        SingleOutputStreamOperator<Tuple3<String,String,Long>> mapWithWatermark = map.assignTimestampsAndWatermarks(watermarkStrategy);
        // 5. 做keyBy
        KeyedStream<Tuple3<String,String,Long>, String> kyStream = mapWithWatermark.keyBy(new KeySelectorFunction());
        SingleOutputStreamOperator<String> process = kyStream.process(new KeyedProcessFunction<>() {

            // 1）定义MapState值
            MapState<String,Long> currentValue;

            @Override
            public void open(OpenContext openContext) throws Exception {
                super.open(openContext);
                // 2）初始化MapState值
                MapStateDescriptor<String,Long> descriptor = new MapStateDescriptor<>("currentValue",Types.STRING, Types.LONG);
                currentValue = getRuntimeContext().getMapState(descriptor);
            }

            @Override
            public void processElement(Tuple3<String, String, Long> value, KeyedProcessFunction<String, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                // 3）获取某个key值的LMapState数据
                Long num = currentValue.get(value.f1);
                if(num==null){
                    num = value.f2;
                }else {
                    num = num + value.f2;
                }
                // 4）更新LMapState数据
                currentValue.put(value.f1, num);
                // 4）获取所有LMapState数据
                Iterator<Map.Entry<String, Long>> iterator = currentValue.iterator();
                StringBuilder sb = new StringBuilder();
                sb.append("==== key=").append(value.f0).append(" start ======\n");
                while (iterator.hasNext()){
                    Map.Entry<String, Long> next = iterator.next();
                    sb.append("key=").append(next.getKey()).append(", value=").append(next.getValue()).append("\n");
                }
                sb.append("==== key=").append(value.f0).append(" end ======\n");
                out.collect(sb.toString());
            }

        });
        // 6. 打印
        process.print();
        // 执行
        env.execute();
    }


    public static class Tuple3MapFunction implements MapFunction<String, Tuple3<String,String,Long>> {

        @Override
        public Tuple3<String, String, Long> map(String value) throws Exception {
            String[] values = value.split(",");
            String value1 = values[0];
            String value2 = "";
            long value3 = 0;
            if(values.length >= 2){
                value2 = values[1];
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

    public static class KeySelectorFunction implements KeySelector<Tuple3<String,String,Long>, String> {

        @Override
        public String getKey(Tuple3<String,String,Long> value) throws Exception {
            // 返回第一个值，作为keyBy的分类
            return value.f0;
        }

    }
}
