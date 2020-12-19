package com.exercise;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author cqzhang
 * @create 2020-12-15 8:47
 */
public class EventTimeprocess {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //从端口读取数据生成数据流
        DataStreamSource<String> socketDataStream = env.socketTextStream("hadoop102", 9999);
        //设置watermark窗口延迟时间和事件时间
        SingleOutputStreamOperator<String> waterStream = socketDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });
        //转换数据为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = waterStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Tuple2<>(fields[0], 1);
            }
        });
        //keyby转换为
        SingleOutputStreamOperator<Tuple2<String, Integer>> side_out_put = mapStream.keyBy(0)
                .timeWindow(Time.seconds(30), Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("side_out_put"){})
                .sum(1);

        side_out_put.print("result");
        side_out_put.getSideOutput(new OutputTag<Tuple2<String, Integer>>("side_out_put"){}).print("sideoutput:");
        env.execute("");
    }
}
