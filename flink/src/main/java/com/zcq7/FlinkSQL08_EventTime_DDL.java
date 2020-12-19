package com.zcq7;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author cqzhang
 * @create 2020-12-18 20:40
 */
public class FlinkSQL08_EventTime_DDL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.setParallelism(1);

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String sinkDDL= "CREATE TABLE user_actions (" +
                "  id VARCHAR(20)," +
                "  ts BIGINT," +
                "  temp DOUBLE," +
                "  user_action_time as TO_TIMESTAMP( FROM_UNIXTIME(ts)) ," +
                "  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND" +
                ") WITH (" +
                "  'connector.type' = 'filesystem'," +
                "  'connector.path' = 'file:///D:/bigdatademo2/flink/sensor', " +
                "  'format.type' = 'csv')";

        bsTableEnv.sqlUpdate(sinkDDL);

        Table table = bsTableEnv.sqlQuery("select * from user_actions");

        bsTableEnv.toAppendStream(table, Row.class).print();
        table.printSchema();

        bsEnv.execute();
    }
}
