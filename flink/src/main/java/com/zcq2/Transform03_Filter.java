package com.zcq2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cqzhang
 * @create 2020-12-11 18:05
 */
public class Transform03_Filter {
    public static void main(String[] args) throws Exception {
        //1.创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.创建DataStream
        DataStreamSource<String> socketStream = env.socketTextStream("hadoop102", 9999);

        //3.Filter过滤
        SingleOutputStreamOperator<String> filterStream = socketStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.length() > 2;
            }
        });
        //4.打印输出
        filterStream.print();
        //5.执行任务
        env.execute("Transform03_Filter");
    }
}
