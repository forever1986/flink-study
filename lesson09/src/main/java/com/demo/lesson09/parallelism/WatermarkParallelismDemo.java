package com.demo.lesson09.parallelism;

import com.demo.lesson09.util.KeySelectorFunction;
import com.demo.lesson09.util.ServerInfo;
import com.demo.lesson09.util.ServerInfoMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * 演示多并行度的情况
 */
public class WatermarkParallelismDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置2个并行度演示
        env.setParallelism(2);
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. map做类型转换
        SingleOutputStreamOperator<ServerInfo> map = text.map(new ServerInfoMapFunction());
        // 4. 定义乱序watermark以及TimestampAssigner
        WatermarkStrategy<ServerInfo> watermarkStrategy = WatermarkStrategy
                // 设置乱序watermark，需要指定水位线延迟时间，设置2秒
                .<ServerInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                // 设置事件时间处理器
                .withTimestampAssigner((element, recordTimestamp) ->{
                    System.out.println("event time = "+element.getTime()+ " 初始时间戳" + recordTimestamp);
                    return element.getTime() * 1000L;
                })
                // 空闲等待时间，如果上游多个并行度，有一个没有过来，超过10秒钟就废弃那个没过来的并行度watermark，直接取已经过来的最小值
//                .withIdleness(Duration.ofSeconds(10))
                ;
        SingleOutputStreamOperator<ServerInfo> mapWithWatermark = map.assignTimestampsAndWatermarks(watermarkStrategy);
        // 5. 做keyBy
        KeyedStream<ServerInfo, String> kyStream = mapWithWatermark.keyBy(new KeySelectorFunction());
        // 6. 开窗 - 滚动窗口，必须是TumblingEventTimeWindows，5秒分割一个窗口
        WindowedStream<ServerInfo, String, TimeWindow> windowStream = kyStream.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)));
        // 7. 计算
        SingleOutputStreamOperator<String> process = windowStream.process(new ProcessWindowFunction<ServerInfo, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<ServerInfo, String, String, TimeWindow>.Context context, Iterable<ServerInfo> elements, Collector<String> out) throws Exception {
                // 打印窗口的开始时间和结束时间
                System.out.println("process子任务id=" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()+ "该窗口的时间："+ DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT.format(context.window().getStart())
                        + " - " +DateFormatUtils.ISO_8601_EXTENDED_DATETIME_TIME_ZONE_FORMAT.format(context.window().getEnd())
                        +" 的条数=" + elements.spliterator().estimateSize() + "水位线=" + context.currentWatermark());
                // 平均cpu值
                Iterator<ServerInfo> iterator = elements.iterator();
                double cpu = 0l;
                long num = 0;
                while (iterator.hasNext()){
                    cpu = cpu + iterator.next().getCpu();
                    num ++;
                }
                String result = "平均cpu值: "+ (cpu/num);
                out.collect(result);
            }
        });
        // 8. 打印
        process.print();
        // 执行
        env.execute();
    }
}
