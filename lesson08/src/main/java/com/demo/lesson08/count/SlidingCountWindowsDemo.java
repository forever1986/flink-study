package com.demo.lesson08.count;

import com.demo.lesson08.method.ReduceDemo;
import com.demo.lesson08.model.ServerInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * 计数滑动窗口示例
 */
public class SlidingCountWindowsDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. map做类型转换
        SingleOutputStreamOperator<ServerInfo> map = text.map(new ReduceDemo.ServerInfoMapFunction());
        // 4. 做keyBy
        KeyedStream<ServerInfo, String> kyStream = map.keyBy(new ReduceDemo.KeySelectorFunction());
        // 5. 开窗 - 活动窗口，间隔为3条数据
        WindowedStream<ServerInfo, String, GlobalWindow> windowedStream = kyStream.countWindow(4,2);
        // 6. 计算 - 计算平均值
        SingleOutputStreamOperator<String> process = windowedStream.process(new ProcessWindowFunction<>() {
            @Override
            public void process(String s, ProcessWindowFunction<ServerInfo, String, String, GlobalWindow>.Context context, Iterable<ServerInfo> elements, Collector<String> out) throws Exception {
                long num = elements.spliterator().estimateSize();
                Iterator<ServerInfo> iterator = elements.iterator();
                double sum = 0l;
                while (iterator.hasNext()) {
                    sum = sum + iterator.next().getCpu();
                }
                out.collect("cpu平均值=" + (sum / num) + " 条数=" + num);
            }
        });
        // 7. 打印
        process.print();
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
