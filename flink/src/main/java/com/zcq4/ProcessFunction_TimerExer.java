package com.zcq4;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 需求：监控温度传感器的温度值，如果温度值在10秒钟之内(processing time)没有下降，则报警。
 *
 * @author cqzhang
 * @create 2020-12-14 21:04
 */
public class ProcessFunction_TimerExer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDataStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> mapStream = socketDataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });
        KeyedStream<SensorReading, String> keyedStream = mapStream.keyBy(SensorReading::getId);

        SingleOutputStreamOperator<String> process = keyedStream.process(new MyMapFunc(10L));
        process.print();


        env.execute("");
    }

    private static class MyMapFunc extends KeyedProcessFunction<String, SensorReading,String> {
        private long threshold;

        public MyMapFunc(long thres) {
            this.threshold = thres;
        }
        private ValueState<Double> lastTempState;
        @Override
        public void open(Configuration parameters) throws Exception {
            //获取lastState
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("Temp-state", Double.class));

        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = lastTempState.value();
            lastTempState.update(value.getTemp());
            long processingTime = ctx.timerService().currentProcessingTime();
            if (lastTemp!=null && lastTemp < value.getTemp()){
                //获取当前需要注册的注册器定时时间
                long ts = ctx.timerService().currentProcessingTime() + threshold * 1000L;
                //注册当前key的processing time定时器,当processingtime到达ts时间时，触发ontimer回调函数
                ctx.timerService().registerEventTimeTimer(ts);
            }else if (lastTemp!=null && lastTemp>value.getTemp()){

            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("s");
        }
    }
}
