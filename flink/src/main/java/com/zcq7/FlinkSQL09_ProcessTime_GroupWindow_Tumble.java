package com.zcq7;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author cqzhang
 * @create 2020-12-18 23:08
 */
public class FlinkSQL09_ProcessTime_GroupWindow_Tumble {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> mapStream = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        Table table = tabEnv.fromDataStream(mapStream,"id,ts,temp,pt.proctime");

        Table tableResult = table.window(Tumble.over("10.seconds").on("pt").as("tw"))
                .groupBy("tw,id")
                .select("id,id.count,tw.start");

        tabEnv.toAppendStream(tableResult, Row.class).print();
        env.execute();
    }
}
