package com.zcq2;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author cqzhang
 * @create 2020-12-11 20:34
 */
public class Transform08_Connect_CoMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> mapStream = socketStream.map((MapFunction<String, SensorReading>) value -> {
            String[] sensors = value.split(",");
            return new SensorReading(sensors[0], Long.parseLong(sensors[1]), Double.parseDouble(sensors[2]));
        });
        SplitStream<SensorReading> splitStream = mapStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemp() > 30.0 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Double>> high = splitStream.select("high").map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemp());
            }
        });
        DataStream<SensorReading> low = splitStream.select("low");
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = high.connect(low);
        SingleOutputStreamOperator<Object> res = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return value;
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return value;
            }
        });
        res.print();
        env.execute();
    }
}
