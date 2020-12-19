package com.zcq7;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author cqzhang
 * @create 2020-12-18 18:59
 */
public class FlinkSQL05_ProcessingTime_DDL_FromexternalSystem {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.setParallelism(1);

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String sinkDDL = "CREATE TABLE MyUserTable (" +
                " id varchar(20)," +
                " ts bigint," +
                " temp double," +
                " pt as proctime()" +
                ") WITH (" +
                "  'connector.type' = 'filesystem'," +
                "  'connector.path' = 'file:///D:/bigdatademo2/flink/sensor', " +
                "  'format.type' = 'csv')";

        //DDL建表指定proctime search  -->Streaming Concepts -->Time Attributes -->Defining in create table DDL
        //with 后的search ---  Connect to External Systems--> Table Connectors -->e.g. File System Connector

        bsTableEnv.sqlUpdate(sinkDDL);
        Table myUserTable = bsTableEnv.from("MyUserTable");
        Table select = myUserTable.select("*");
        bsTableEnv.toAppendStream(select,Row.class).print();


        String querySQL = "SELECT id,ts,pt " +
                "        FROM MyUserTable" ;

        Table sqlres = bsTableEnv.sqlQuery(querySQL);

        bsTableEnv.toRetractStream(sqlres, Row.class).print();

        bsEnv.execute();
    }
}
