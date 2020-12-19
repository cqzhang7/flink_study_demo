package com.zcq2;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author cqzhang
 * @create 2020-12-14 9:09
 */
public class FlinkExer_window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketDataStream = env.socketTextStream("hadoop102", 9999).setParallelism(1);


        //先转为SensorReading再设置watermark会导致watermark轮询发送至map并行task度中，后续的watermark更新不及时
        SingleOutputStreamOperator<SensorReading> mapStream = socketDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTs() * 1000L;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Double>> tuple2SingleOutputStreamOperator = mapStream
                .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading value) throws Exception {
                        return new Tuple2<>(value.getId(), value.getTemp());
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(30), Time.seconds(5))
                .sum(1);
        //double的sum结果小数位都是0？？
        // @@@@:10> (sensor_1,34.0)
        //@@@@:10> (sensor_1,34.0)
        //@@@@:10> (sensor_1,64.0)
        //@@@@:10> (sensor_1,180.8)
        tuple2SingleOutputStreamOperator.print("@@@@");
        env.execute("");
    }
}
