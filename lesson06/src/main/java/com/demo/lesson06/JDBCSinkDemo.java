package com.demo.lesson06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JDBCSinkDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据-来自socket
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        // 3. 将数据转换为3元组
        SingleOutputStreamOperator<Tuple3<String, Double, Long>> map = source.map(new Tuple3MapFunction());
        // 4. 写入到mysql数据库：注意这里JdbcSink.sink()有2个方法，一个是基于SinkV1版本，一个是使用SinkV2版本。这里使用SinkV2版本
        JdbcSink<Tuple3<String, Double, Long>> jdbcSink = JdbcSink.<Tuple3<String, Double, Long>>builder()
                .withQueryStatement(
                        "insert into log_table values(?,?,?)",
                        (JdbcStatementBuilder<Tuple3<String, Double, Long>>) (preparedStatement, value) -> {
                            preparedStatement.setString(1, value.f0);
                            preparedStatement.setDouble(2, value.f1);
                            preparedStatement.setLong(3, value.f2);
                        })
                .withExecutionOptions(JdbcExecutionOptions.builder()
                        .withMaxRetries(3) // 重试次数
                        .withBatchSize(10) // 每次插入的批次数量
                        .withBatchIntervalMs(1000) // 批次插入间隔时间
                        .build())
                .buildAtLeastOnce(new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        // 数据库连接
                        .withUrl("jdbc:mysql://127.0.0.1:3306/flink_test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&useSSL=false&allowPublicKeyRetrieval=true")
                        .withUsername("root") // 账号
                        .withPassword("root") // 密码
                        .build());
        // sinkTo就是SinkV2版本
        map.sinkTo(jdbcSink);
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
