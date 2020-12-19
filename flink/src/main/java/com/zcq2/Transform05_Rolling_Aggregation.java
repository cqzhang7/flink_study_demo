package com.zcq2;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cqzhang
 * @create 2020-12-11 18:22
 */
public class Transform05_Rolling_Aggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource<String> socketStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> mapStream = socketStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] sensors = value.split(",");
                SensorReading sensorReading = new SensorReading(sensors[0], Long.parseLong(sensors[1]), Double.parseDouble(sensors[2]));
                return sensorReading;
            }
        });
        KeyedStream<SensorReading, Tuple> keyedStream= mapStream.keyBy("id");
        SingleOutputStreamOperator<SensorReading> maxTemp = keyedStream.max("temp");
        maxTemp.print();


        env.execute();
    }
}
