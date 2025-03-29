package com.demo.lesson05.base;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 假设是一个监听来自服务器cpu的日志，日志格式是“服务器id,cpu,时间
 * 使用一个RichMapFunction 算子，将服务器CPU日志转换为一个三元组，并输出。
 * （中间验收open和close方法以及RuntimeContext能获取到的信息）
 */
public class RichFunctionDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 计算
        DataStream<Tuple3<String,Double,Long>> richMap = text.map(new RichMapFunctionDemo());
        // 4. 输出
        richMap.print();
        // 执行
        env.execute();
    }


    public static class RichMapFunctionDemo extends RichMapFunction<String, Tuple3<String,Double,Long>> {

        @Override
        public Tuple3<String, Double, Long> map(String value) throws Exception {
            // 任务id
            System.out.println("子任务id="+getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()
                    + " 其所在作业id=" +getRuntimeContext().getJobInfo().getJobId());
            // 任务名称
            System.out.println("子任务id="+getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()
                    + " 其所在作业名称=" +getRuntimeContext().getJobInfo().getJobName());
            // 子任务的序号
            System.out.println("子任务id="+getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()
                    + " 其所在子任务id=" +getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
            // 子任务的名称
            System.out.println("子任务id="+getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()
                    + " 其所在子任务名称=" +getRuntimeContext().getTaskInfo().getTaskName());
            // 算子的并行度
            System.out.println("子任务id="+getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()
                    + " 其所在算子的并行度=" +getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks());

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

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            System.out.println(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()+" open ...");
        }

        /**
         * 注意：close只有作业在正常关闭时才会调用，比如cancel job、作业执行完毕、子任务抛出异常导致作业结束都会触发close，但是要是你程序宕机的或者直接断电，是不会调用的。
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            super.close();
            System.out.println(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()+" close ...");
        }
    }

}
