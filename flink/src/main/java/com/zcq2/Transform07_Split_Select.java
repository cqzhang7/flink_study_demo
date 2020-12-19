package com.zcq2;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * @author cqzhang
 * @create 2020-12-11 20:09
 */
public class Transform07_Split_Select {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource<String> socketStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> mapStream = socketStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] strings = value.split(",");
                return new SensorReading(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
            }
        });
        SplitStream<SensorReading> splitStream = mapStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                //为什么用Collections？
                return sensorReading.getTemp() > 30.0 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });
        DataStream<SensorReading> high = splitStream.select("high");
        DataStream<SensorReading> low = splitStream.select("low");
        DataStream<SensorReading> all = splitStream.select("high", "low");

        high.print("high:");
        low.print("low:");
        all.print("all:");

        env.execute();
    }
}
