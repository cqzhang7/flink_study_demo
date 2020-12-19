package com.zcq5;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 需求：同一个传感器连续10s温度不下降报警
 * @author cqzhang
 * @create 2020-12-15 23:09
 */
public class Flink01_State_OnTimer_TempNotDesc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> socketDataStream = env.socketTextStream("hadoop102", 9999);
        //使用事件时间
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = socketDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                return Long.parseLong(element.split(",")[1]) * 1000L;
            }
        });

        SingleOutputStreamOperator<SensorReading> mapStream = stringSingleOutputStreamOperator.map(record -> {
            String[] fields = record.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });
        SingleOutputStreamOperator<String> processStream = mapStream.keyBy(SensorReading::getId)
                .process(new MyProcess(10));

        processStream.print("res");

        env.execute("");

    }

    private static class MyProcess extends KeyedProcessFunction<String,SensorReading,String> {

        private int interval;
        private ValueState<Double> tempState;
        private ValueState<Long> timeState;

        public MyProcess(int interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            tempState = getRuntimeContext().
                    getState(new ValueStateDescriptor<Double>("Temp-state", Double.class));
            timeState = getRuntimeContext().
                    getState(new ValueStateDescriptor<Long>("time-state", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = tempState.value();
            Double temp = value.getTemp();
            tempState.update(temp);
            Long lastts = timeState.value();
            if (lastTemp == null || (lastTemp < temp && lastts == null)){
                long ts = ctx.timerService().currentWatermark() + interval * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timeState.update(ts);
            }else if (lastTemp>temp && lastts != null){
                ctx.timerService().deleteEventTimeTimer(lastts);
                timeState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            timeState.clear();
            out.collect("温度连续"+interval+"秒上升，请注意！");

        }
    }
}
