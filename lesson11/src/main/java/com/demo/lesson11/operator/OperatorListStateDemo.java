package com.demo.lesson11.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorListStateDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. map做类型转换
        SingleOutputStreamOperator<Double> map = text.map(new DoubleMapFunction());
        // 5. 打印
        map.print();
        // 执行
        env.execute();
    }


    public static class DoubleMapFunction implements MapFunction<String, Double>, CheckpointedFunction {

        private Double cpu;
        private ListState<Double> cpuState;

        @Override
        public Double map(String value) throws Exception {
            String[] values = value.split(",");
            double value2 = Double.parseDouble("0");
            if(values.length >= 2){
                try {
                    value2 = Double.parseDouble(values[1]);
                }catch (Exception e){
                    value2 = Double.parseDouble("0");
                }
            }
            cpu = cpu + value2;
            return cpu;
        }

        /**
         * 初始化算子状态
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState...");
            ListStateDescriptor<Double> descriptor = new ListStateDescriptor<>("cpu", Types.DOUBLE);
            cpuState = context.getOperatorStateStore().getListState(descriptor);
            if(context.isRestored()){
                cpu = cpuState.get().iterator().next();
            }else{
                cpu = 0.0;
            }
        }

        /**
         * 在检查点触发时，存储算子状态
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState...");
            cpuState.clear();
            cpuState.add(cpu);
        }
    }
}
