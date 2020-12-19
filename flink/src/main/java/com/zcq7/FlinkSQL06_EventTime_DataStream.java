package com.zcq7;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author cqzhang
 * @create 2020-12-18 20:40
 */
public class FlinkSQL06_EventTime_DataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketDataStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<SensorReading> mapStream = socketDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                return Long.parseLong(element.split(",")[1]) * 1000L;
            }
        }).map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0],
                        Long.parseLong(fields[1]),
                        Double.parseDouble(fields[2]));
            }
        });
        //DataStream<Tuple2<String, String>>用Tuple2转化时直接对Tuple的两个值定义字段名即可

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        Table table = tabEnv.fromDataStream(mapStream, "id, ts, temp, user_action_time.rowtime");

        GroupWindowedTable window = table.window(Tumble.over("10.seconds").on("user_action_time").as("userActionWindow"));

        //GroupBy must contain exactly one window alias.   ---->userActionWindow
        Table res = window.groupBy("id,userActionWindow").select("id,temp.sum");

        //root
        // |-- id: STRING
        // |-- ts: BIGINT
        // |-- temp: DOUBLE
        // |-- user_action_time: TIMESTAMP(3) *ROWTIME*
        table.printSchema();
        tabEnv.toAppendStream(res, Row.class).print();
        env.execute("");
    }
}
