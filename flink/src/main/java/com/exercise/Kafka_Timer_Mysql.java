package com.exercise;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author cqzhang
 * @create 2020-12-16 8:39
 */
public class Kafka_Timer_Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<SensorReading> mapStream = kafkaStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });
        SingleOutputStreamOperator<String> processStream = mapStream.keyBy(SensorReading::getId)
                .process(new MyPro(10));
        processStream.print("count");
        processStream.getSideOutput(new OutputTag<String>("warnmsg"){}).print("sideout");


        env.execute("");
    }

    private static class MyPro extends KeyedProcessFunction<String, SensorReading, String> {
        private int interval;
        private ValueState<Integer> countState;
        private ValueState<Long> timeState;
        private ValueState<Double> tempState;

        public MyPro(int interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("sonser-count", Integer.class));
            timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeState", Long.class));
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("tempState", Double.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Integer lastCount = countState.value();
            if (lastCount == null) {
                lastCount = 0;
            }
            countState.update(lastCount + 1);
            Long lastTime = timeState.value();
            Double lasttemp = tempState.value();
            Double curTemp = value.getTemp();
            tempState.update(curTemp);
            if (lasttemp == null ||(lasttemp<curTemp && lastTime == null)){
                long ts = ctx.timerService().currentProcessingTime() + interval;
                ctx.timerService().registerEventTimeTimer(ts);
                timeState.update(ts);
            }else if (lasttemp>curTemp && lastTime != null){
                ctx.timerService().deleteEventTimeTimer(lastTime);
                timeState.clear();
            }
            out.collect(countState.value().toString());

        }
        //timestamp：注册定时器的时间
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            String warnmsg = ctx.getCurrentKey() + ":温度超过" + interval + "秒没有下降!";
            ctx.output(new OutputTag<String>("warnmsg"){},warnmsg);
        }
    }
}
