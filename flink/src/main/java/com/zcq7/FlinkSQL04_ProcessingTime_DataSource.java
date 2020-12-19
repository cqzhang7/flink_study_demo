package com.zcq7;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.elasticsearch.common.recycler.Recycler;

/**
 * @author cqzhang
 * @create 2020-12-18 18:15
 */
public class FlinkSQL04_ProcessingTime_DataSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        tabEnv.connect(new FileSystem().path("flink/sensor"))
                .withFormat(new Csv())
                .withSchema(new Schema().field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE())
                .field("pt",DataTypes.TIMESTAMP(3)).proctime())
                .createTemporaryTable("sensorTable");

        Table sensorTable = tabEnv.from("sensorTable");
        //从外部连接器直接获取数据创建表，指定processtime在schema中指定

        //↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓从ConnectorDescriptor连接时指定的pt，不能被选取打印 ----用新版？？Blink流？？↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓

        //Caused by: org.apache.flink.streaming.runtime.tasks.AsynchronousException: Caught exception when processing split: null
        //Table filtertable = sensorTable.select("id,ts,temp,pt").filter("ts % 2 =0");
        Table filtertable = sensorTable.select("id,ts,temp").filter("ts % 2 =0");


        sensorTable.printSchema();
        tabEnv.toAppendStream(filtertable, Row.class).print();

        env.execute("");
    }
}
