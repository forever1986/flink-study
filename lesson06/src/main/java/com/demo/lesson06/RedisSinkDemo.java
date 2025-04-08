package com.demo.lesson06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RedisSinkDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据-来自socket
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Double>> map = source.map(new Tuple2MapFunction());
        // 3. 输出到redis中
        RedisSink<Tuple2<String, Double>> redisSink = new RedisSink<>(
                // 这是配置Redis的连接信息
                new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build(),
                // 这是配置数据格式，我们自己实现一个类RedisExampleMapper
                new RedisExampleMapper(RedisCommand.SET)
        );
        // 这里使用addSink,而非sinkTo，是因为目前RedisSink还是返回SinkFunction，没有改造成Sink
        map.addSink(redisSink);
        // 执行
        env.execute();
    }

    public static class RedisExampleMapper implements RedisMapper<Tuple2<String, Double>> {

        // 执行Redis的命令，我们从外面传入RedisCommand.SET，意味着我们使用String格式存储
        private RedisCommand redisCommand;

        public RedisExampleMapper(RedisCommand redisCommand){
            this.redisCommand = redisCommand;
        }

        // 设置命令，也是定义类型
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(redisCommand);
        }

        // 设置key
        @Override
        public String getKeyFromData(Tuple2<String, Double> data) {
            return "flink-"+data.f0;
        }

        // 设置value
        @Override
        public String getValueFromData(Tuple2<String, Double> data) {
            return data.f1.toString();
        }
    }

    public static class Tuple2MapFunction implements MapFunction<String, Tuple2<String,Double>> {

        @Override
        public Tuple2<String, Double> map(String value) throws Exception {
            String[] values = value.split(",");
            String value1 = values[0];
            double value2 = Double.parseDouble("0");
            if(values.length >= 2){
                try {
                    value2 = Double.parseDouble(values[1]);
                }catch (Exception e){
                    value2 = Double.parseDouble("0");
                }
            }
            return new Tuple2<>(value1,value2);
        }
    }
}
