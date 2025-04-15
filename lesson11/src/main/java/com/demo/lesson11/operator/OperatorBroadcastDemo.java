package com.demo.lesson11.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class OperatorBroadcastDemo {

    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 2. 读取数据
        DataStreamSource<String> dataSource = env.socketTextStream("127.0.0.1", 8888);// 服务器cpu数据流
        DataStreamSource<String> configSource = env.socketTextStream("127.0.0.1", 9999);// cpu报警值数据流
        // 3. map做类型转换
        SingleOutputStreamOperator<Tuple3<String,Double,Long>> dataMap = dataSource.map(new Tuple3MapFunction());

        MapStateDescriptor<String, Double> descriptor = new MapStateDescriptor<>("broadcast", Types.STRING, Types.DOUBLE);
        BroadcastStream<String> broadcast = configSource.broadcast(descriptor);
        SingleOutputStreamOperator<String> process = dataMap.connect(broadcast).process(new BroadcastProcessFunction<Tuple3<String, Double, Long>, String, String>() {

            private final String WARN_VALUE_KEY = "warnValue";
            private final double DEFAULT_WARN_VALUE = 80;

            @Override
            public void processElement(Tuple3<String, Double, Long> value, BroadcastProcessFunction<Tuple3<String, Double, Long>, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String, Double> broadcastState = ctx.getBroadcastState(descriptor);
                Double warnValue = broadcastState.get(WARN_VALUE_KEY);
                if (warnValue==null){
                    warnValue = DEFAULT_WARN_VALUE;
                }
                if(warnValue.compareTo(value.f1)<=0){
                    out.collect("服务器id=" + value.f0 + ", 时间="+ value.f2+", cpu="+ value.f1 + " 超过了警戒值" + warnValue + ", 发生报警！！！！");
                }

            }

            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<Tuple3<String, Double, Long>, String, String>.Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, Double> broadcastState = ctx.getBroadcastState(descriptor);
                double warnValue = DEFAULT_WARN_VALUE;
                try {
                    warnValue = Double.parseDouble(value);
                }catch (Exception ignored){
                    // 异常不做处理，默认80
                }
                // 将值放入到广播中
                broadcastState.put(WARN_VALUE_KEY, warnValue);
            }
        });
        // 5. 打印
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
}
