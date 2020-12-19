package com.zcq2;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cqzhang
 * @create 2020-12-11 21:26
 */
public class Rich_Functions {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        socketTextStream.map(new MyRichMap());
    }

    public static class MyRichMap extends RichMapFunction<String, SensorReading>{

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public SensorReading map(String value) throws Exception {
            return null;
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}

