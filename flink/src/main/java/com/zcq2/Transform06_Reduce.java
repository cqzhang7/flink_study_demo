package com.zcq2;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cqzhang
 * @create 2020-12-11 19:59
 */
public class Transform06_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource<String> socketStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> mapStream = socketStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] words = value.split(",");
                return new SensorReading(words[0], Long.parseLong(words[1]), Double.parseDouble(words[2]));
            }
        });
        KeyedStream<SensorReading, Tuple> keyedStream = mapStream.keyBy("id");
        SingleOutputStreamOperator<SensorReading> reduceStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value2.getId(),
                        value2.getTs(),
                        Math.max(value1.getTemp(), value2.getTemp()));
            }
        });
        reduceStream.print();
        env.execute();
    }
}
