package com.demo.lesson05.partition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将输入的数据，分组轮询分配到下游print算子打印
 */
public class RescaleOperatorDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 为了演示效果，我们使用一个map设置2个并行度，然后分组轮询分配到4个并行度的输出print算子
        SingleOutputStreamOperator<String> map = text.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println("map:" + getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() + ">" + value);
                return value;
            }
        }).setParallelism(2);
        map.rescale().print("print").setParallelism(4);
        // 执行
        env.execute();
    }

}
