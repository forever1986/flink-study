package com.demo.lesson08.method;

import com.demo.lesson08.model.ServerAvgInfo;
import com.demo.lesson08.model.ServerInfo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * 演示Aggregate方法
 */
public class AggregateDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. map做类型转换
        SingleOutputStreamOperator<ServerInfo> map = text.map(new ReduceDemo.ServerInfoMapFunction());
        // 4. 做keyBy
        KeyedStream<ServerInfo, String> kyStream = map.keyBy(new ReduceDemo.KeySelectorFunction());
        // 5. 开窗
        WindowedStream<ServerInfo, String, TimeWindow> windowStream = kyStream.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)));
        // 6. 计算
        SingleOutputStreamOperator<String> reduce = windowStream.aggregate(new AggregateFunction<ServerInfo, ServerAvgInfo, String>() {
            @Override
            public ServerAvgInfo createAccumulator() {
                // 初始值
                System.out.println("====createAccumulator====");
                return new ServerAvgInfo();
            }

            @Override
            public ServerAvgInfo add(ServerInfo value, ServerAvgInfo accumulator) {
                System.out.println("====add====");
                // 累积cpu值以及条数
                accumulator.setServerId(value.getServerId());
                accumulator.setNum(accumulator.getNum()==null?1:accumulator.getNum()+1);
                accumulator.setCpuTotal(accumulator.getCpuTotal()==null?value.getCpu():accumulator.getCpuTotal()+ value.getCpu());
                return accumulator;
            }

            @Override
            public String getResult(ServerAvgInfo accumulator) {
                System.out.println("====getResult====");
                // 平均cpu值
                return "平均cpu值: "+(accumulator.getCpuTotal()/accumulator.getNum());
            }

            @Override
            public ServerAvgInfo merge(ServerAvgInfo a, ServerAvgInfo b) {
                System.out.println("====merge====");
                return null;
            }
        });
        // 7. 打印
        reduce.print();
        // 执行
        env.execute();
    }

    public static class ServerInfoMapFunction implements MapFunction<String, ServerInfo> {

        @Override
        public ServerInfo map(String value) throws Exception {
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
            return new ServerInfo(value1,value2,value3);
        }
    }

    public static class KeySelectorFunction implements KeySelector<ServerInfo, String> {
        @Override
        public String getKey(ServerInfo value) throws Exception {
            // 返回第一个值，作为keyBy的分类
            return value.getServerId();
        }
    }
}
