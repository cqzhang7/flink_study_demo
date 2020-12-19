package com.zcq4;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author cqzhang
 * @create 2020-12-14 15:42
 */
public class ProcessFunction01_test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDataStream = env.socketTextStream("hadoop102", 9999);
        
        socketDataStream.process(new MyProcessFunc());

        env.execute("");

    }

    public static class MyProcessFunc extends ProcessFunction<String, String> {
        //声明周期方法
        @Override
        public void open(Configuration parameters) throws Exception {
            //获取运行时上下文，做状态编程
            RuntimeContext runtimeContext = getRuntimeContext();
//            runtimeContext.getState()
            ValueState<Double> sss = getRuntimeContext().getState(new ValueStateDescriptor<Double>("sss", Double.class));
            //此操作！！！不可，因为是keyedprocess，update需要指定key操作，open中对应
            sss.update(22.3);
        }
        //定时器触发任务执行
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        }

        //处理进入系统的每一条数据
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            //输出数据
            out.collect("");

            //获取处理时间相关数据
            long processingTime = ctx.timerService().currentProcessingTime();
            //注册定时器
            ctx.timerService().registerProcessingTimeTimer(1L);
            //删除定时器
            ctx.timerService().deleteProcessingTimeTimer(1L);

            //获取事件时间相关数据
            ctx.timerService().currentWatermark();
            //注册和删除定时器
            ctx.timerService().registerEventTimeTimer(1L);
            ctx.timerService().deleteEventTimeTimer(1L);

            //侧输出流
            ctx.output(new OutputTag<String>("sideOutput"),"");
        }

        //声明周期方法
        @Override
        public void close() throws Exception {
            super.close(

            );
        }
    }
}
