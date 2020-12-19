package com.zcq6;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author cqzhang
 * @create 2020-12-17 15:14
 */
public class FlinkSQL_Source_file {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(new FileSystem().path("flink/sensor"))
                //目前csv和oldcsv都可以
                .withFormat(new Csv())
                .withSchema(new Schema().field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("sensor");

        Table sensorTable = tableEnv.from("sensor");
        Table tableres = sensorTable.select("id,ts,temp").where("temp > 29");
        Table sqlres = tableEnv.sqlQuery("select * from sensor");

        //每次处理一条数据：
        //table> sensor_1,1547718199,35.8
        //sql:> sensor_1,1547718199,35.8
        //sql:> sensor_6,1547718201,15.4
        //table> sensor_10,1547718205,38.1
        //sql:> sensor_7,1547718202,6.7
        //table> sensor_1,1547718200,36.5
        //sql:> sensor_10,1547718205,38.1
        //sql:> sensor_1,1547718200,36.5
        //sql:> sensor_1,1547718201,34.2
        //table> sensor_1,1547718201,34.2
        tableEnv.toAppendStream(tableres, Row.class).print("table");
        tableEnv.toAppendStream(sqlres,Row.class).print("sql:");

        env.execute();

    }
}
