package com.zcq4;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *需求：利用Keyed State，实现这样一个需求：
 *          检测传感器的温度值，如果连续的两个温度差值超过10度，就输出报警。
 * @author cqzhang
 * @create 2020-12-14 18:48
 */

public class ProcessFunction_TempDiff2map {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取socket数据流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //转换为SensorReading流

        SingleOutputStreamOperator<SensorReading> mapStream = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0],
                        Long.parseLong(fields[1]),
                        Double.parseDouble(fields[2]));
            }
        });
        //对流进行分组
        KeyedStream<SensorReading, String> keyStream = mapStream.keyBy(SensorReading::getId);
        SingleOutputStreamOperator<String> map = keyStream.map(new MyRichMap(10L));
        map.print();
        env.execute();

    }
    public static class MyRichMap extends RichMapFunction<SensorReading,String> {
        private Long threshold;
//        private ValueState<Double> lastTempstate;
        private ValueState<Double> state;

        public MyRichMap(Long threshold) {
            this.threshold = threshold;
        }

//        一个批次只运行一次
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            lastTempstate = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state1", Double.class));
//        }


        @Override
        public String map(SensorReading value) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state", Double.class));
            Double statevalue = state.value();
            state.update(value.getTemp());
            if (statevalue != null && Math.abs(value.getTemp()-statevalue)>10){
                return "state温度器温度变化超过"+threshold+"度！";
            }
            return "";
    }
}
}
