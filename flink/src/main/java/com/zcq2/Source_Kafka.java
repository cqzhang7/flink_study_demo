package com.zcq2;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import sun.management.Sensor;

import java.util.Properties;

/**
 * @author cqzhang
 * @create 2020-12-17 10:26
 */
public class Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"flink-source");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");


        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = kafkaSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                return Long.parseLong(element.split(",")[1]) * 1000;
            }
        });
        SingleOutputStreamOperator<SensorReading> mapStream = stringSingleOutputStreamOperator.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });
        SingleOutputStreamOperator<SensorReading> resStream = mapStream.keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(1))
                .sideOutputLateData(new OutputTag<SensorReading>("side-out") {})
                .sum("temp");
        resStream.print("res:");
        resStream.getSideOutput(new OutputTag<SensorReading>("side-out"){}).print("side:");



        env.execute("");
    }
}
