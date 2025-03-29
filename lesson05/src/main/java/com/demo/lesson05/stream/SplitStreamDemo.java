package com.demo.lesson05.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 假设是一个监听来自服务器cpu的日志，日志格式是“服务器id,cpu,时间”
 * 1）当其cpu值大于80且小于95时，我们将数据输出到警告的分流
 * 2）当其cpu大于等于95时，我们将数据输出到错误的分流
 * 3）其它情况正常输出到主流中
 */
public class SplitStreamDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 根据cpu值，做分流处理
        OutputTag<Tuple3<String,Double,Long>> warnOutputTag = new OutputTag<>("warnOutputTag", Types.TUPLE(Types.STRING,Types.DOUBLE,Types.LONG)); // 警告流
        OutputTag<Tuple3<String,Double,Long>> errorOutputTag = new OutputTag<>("errorOutputTag", Types.TUPLE(Types.STRING,Types.DOUBLE,Types.LONG)); // 错误流
        SingleOutputStreamOperator<Tuple3<String, Double, Long>> output = text.map(new Tuple3MapFunction()).process(new ProcessFunction<Tuple3<String, Double, Long>, Tuple3<String, Double, Long>>() {
            @Override
            public void processElement(Tuple3<String, Double, Long> value, Context ctx, Collector<Tuple3<String, Double, Long>> out) throws Exception {
                if (value.f1 > 80 && value.f1 < 95) {
                    //当其cpu值大于80且小于95时，我们将数据输出到警告的分流
                    ctx.output(warnOutputTag, value);
                } else if (value.f1 >= 95) {
                    //当其cpu大于等于95时，我们将数据输出到错误的分流
                    ctx.output(errorOutputTag, value);
                } else {
                    //其它情况正常输出到主流中
                    out.collect(value);
                }
            }
        });
        output.getSideOutput(warnOutputTag).print("警告数据：");// 获取到侧流：警告流
        output.getSideOutput(errorOutputTag).print("错误数据：");// 获取到侧流：错误流
        output.print("正常数据");// 主流输出
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
