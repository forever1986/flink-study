package com.demo.lesson07;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReturnsDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据
        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9999);
        // 3. 计算
        text
                .map((String value) -> {
                    String[] values = value.split(",");
                    String value1 = values[0];
                    double value2 = Double.parseDouble("0");
                    long value3 = 0;
                    if (values.length >= 2) {
                        try {
                            value2 = Double.parseDouble(values[1]);
                        } catch (Exception e) {
                            value2 = Double.parseDouble("0");
                        }
                    }
                    if (values.length >= 3) {
                        try {
                            value3 = Long.parseLong(values[2]);
                        } catch (Exception ignored) {
                        }
                    }
                    return new Tuple3<>(value1, value2, value3);
                })
                // 方法1：如果不知道返回类型，这里会报错
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.LONG))
                // 方法2：使用TypeHint方式
//                .returns(new TypeHint<Tuple3<String,Double,Long>> (){})
                .print();
        // 执行
        env.execute();
    }
}
