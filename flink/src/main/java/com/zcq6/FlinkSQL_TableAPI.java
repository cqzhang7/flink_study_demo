package com.zcq6;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author cqzhang
 * @create 2020-12-16 15:27
 */
public class FlinkSQL_TableAPI {
    public static void main(String[] args) throws Exception {
        //获取执行环境并设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取文本并创建数据流
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("flink/sensor");

        //将每一行数据转换为JAVABEAN
        SingleOutputStreamOperator<SensorReading> mapStream = stringDataStreamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });
        //创建TableAPI执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
        //从流中创建表
        Table table = streamTableEnvironment.fromDataStream(mapStream);
        //TableAPI转换数据为Table（Table再转换生成的表成为view，逻辑表。因为没有数据实体）
        Table tableResult = table.groupBy("id").select("id,temp.max");
        //SQLAPI转换数据,从流中创建表视图
        streamTableEnvironment.createTemporaryView("sensor",mapStream);
        //执行sql生成table对象
        Table sqlResult = streamTableEnvironment.sqlQuery("select id,max(temp) from sensor group by id");

        //转换为流输出数据
        streamTableEnvironment.toRetractStream(tableResult, Row.class).print("table:");
        streamTableEnvironment.toRetractStream(sqlResult,Row.class).print("sql:");

        //启动任务
        env.execute();
    }
}

