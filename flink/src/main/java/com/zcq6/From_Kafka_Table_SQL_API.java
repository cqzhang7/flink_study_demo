package com.zcq6;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectTableDescriptor;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author cqzhang
 * @create 2020-12-16 20:21
 */
public class From_Kafka_Table_SQL_API {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //创建tableenv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"souce-flink");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //创建kafka连接描述器
        Kafka kafka = new Kafka().topic("test").version("0.11").properties(properties).startFromLatest();
        //创建Schema
        Schema schema = new Schema().field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("temp", DataTypes.DOUBLE());
        //通过connector创建表--此方式可创建输入和输出表source sink
        tableEnv.connect(kafka).withFormat(new Csv()).withSchema(schema).createTemporaryTable("sensor");

        Table sqlres = tableEnv.sqlQuery("select id,ts,temp from sensor");

        Kafka kafkaproducer = new Kafka().version("0.11").topic("flink-table-test")
                .property(ProducerConfig.ACKS_CONFIG, "-1")
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                //默认为ByteArraySerializer，即什么都不做
                .property(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
                .property(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        tableEnv.connect(kafkaproducer).withFormat(new Csv()).withSchema(schema).createTemporaryTable("sink2Kafka");
        //写到kafka中
        tableEnv.insertInto("sink2Kafka",sqlres);
        tableEnv.toAppendStream(sqlres,Row.class).print("sql:");

        env.execute();

    }
}
