package com.zcq7;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.elasticsearch.common.recycler.Recycler;

/**
 * @author cqzhang
 * @create 2020-12-18 13:58
 */
public class Flink01_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        Kafka kafka = new Kafka().topic("test").version("0.11")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG, "flink_consumer");
        tabEnv.connect(kafka).withFormat(new Csv())
                .withSchema(new Schema().field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("kafkatable");
        Table kafkatable = tabEnv.from("kafkatable");
        Table tabRes = kafkatable.groupBy("id").select("id,id.count as ct");

//        tabEnv.toRetractStream(tabRes, Row.class).print();
        //JDBCsink没有groupby就是追加流，groupby得有主键，实现更新流



        String sinkDDL = "CREATE TABLE MyUserTable (" +
                "  id varchar(20) not null," +
                "  ct bigint not null" +
                ") WITH (" +
                "  'connector.type' = 'jdbc', " +//-- required: specify this table type is jdbc
                "  'connector.url' = 'jdbc:mysql://hadoop102:3306/flink_test', " +//-- required: JDBC DB url
                "  'connector.table' = 'sensorCT',  "+//-- required: jdbc table name
                "  'connector.driver' = 'com.mysql.jdbc.Driver', " +
                "  'connector.username' = 'root', " +
                "  'connector.password' = '123456', " +
                "  'connector.write.flush.max-rows' = '1' )";//-- 设置刷写时机，一条一刷

        tabEnv.sqlUpdate(sinkDDL);
        tabEnv.insertInto("MyUserTable",tabRes);

        env.execute();

    }
}
