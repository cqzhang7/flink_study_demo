package com.zcq6;

import com.zcq2.bean.WordBean;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Day07_exercise {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        Properties properties = new Properties();
//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"Flink_exer");
//        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
//        Kafka kafka = new Kafka().version("0.11").topic("test").properties(properties);
//        //CSV和Json读取不了非格式化数据
//        tableEnv.connect(kafka).withFormat(new Csv())
//                .withSchema(new Schema().field("wordlist",DataTypes.ARRAY(DataTypes.STRING())))
//                .createTemporaryTable("wordTable");
//
//        Table sqlres = tableEnv.sqlQuery("SELECT wordlist from wordTable");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"Flink_exer");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), properties));



        //2.读取端口数据转换为JavaBean
        SingleOutputStreamOperator<WordBean> wordStream = kafkaStream
                .flatMap(new FlatMapFunction<String, WordBean>() {
                    @Override
                    public void flatMap(String value, Collector<WordBean> out) throws Exception {
                        String[] fields = value.split(" ");
                        for (String field : fields) {
                            out.collect(new WordBean(field));
                        }
                    }
                });

        //3.对流进行注册
        Table table = tableEnv.fromDataStream(wordStream);

        //4.TableAPI
        Table word = table.groupBy("word").select("word,word.count");



        //通过ES连接器注册输出表
        tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("hadoop102", 9200, "http")
                .index("wordCount1")
                .documentType("_doc")
                .bulkFlushMaxActions(1))
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("word", DataTypes.STRING()))
                //inUpsertMode() inRetractMode()
                .inAppendMode()
                .createTemporaryTable("Es");


        tableEnv.insertInto("Es",word);
        //8.执行
        env.execute();

    }

}
