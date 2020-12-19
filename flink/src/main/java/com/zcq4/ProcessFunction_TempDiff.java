package com.zcq4;

import com.zcq2.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 *需求：利用Keyed State，实现这样一个需求：
 *          检测传感器的温度值，如果连续的两个温度差值超过10度，就输出报警。
 * @author cqzhang
 * @create 2020-12-14 18:48
 */

public class ProcessFunction_TempDiff {
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
        //操作：对温度变化超过10度的情况输出报警信息，其他情况不输出
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = keyStream.flatMap(new MyRichFlatMap(10L));

        stringSingleOutputStreamOperator.print();
        env.execute();

    }
    public static class MyRichFlatMap extends RichFlatMapFunction<SensorReading,String>{
        private Long threshold;
        private ValueState<Double> lastTempstate;


        public MyRichFlatMap(Long threshold) {
            this.threshold = threshold;
        }

        //报错:Exception in thread "main" java.lang.IllegalStateException:
        //                      The runtime context has not been initialized.
//        {
//            ValueState<Double> state = getRuntimeContext().getState(new ValueStateDescriptor<Double>("Temp-state", Double.class));
//            System.out.println("hah");
//            try {
//                state.value();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            System.out.println("hend");
//        }




        /**
         * 函数的初始化方法。它在actual working methods（如<i>map</i>或<i>join</i>）之前调用，因此适用于一次设置工作。
         * 对于作为迭代一部分的函数，将在每个迭代superstep的开头调用此方法。
         *
         * <p>传递给函数的配置对象可用于配置和初始化。
         * 	  配置包含在程序组合中的功能上配置的所有参数。
         *
         * <pre>{@code
         * 	  public class MyFilter extends RichFilterFunction<String> {
         *
         * 	      private String searchString;
         *
         * 	      public void open(Configuration parameters) {
         * 	          this.searchString = parameters.getString("foo");
         *                    }
         *
         * 	      public boolean filter(String value) {
         * 	          return value.equals(searchString);
         *          }
         * 	  }
         * }</pre>
         *
         * <p>默认情况下，此方法不执行任何操作。。
         *
         * @see org.apache.flink.configuration.Configuration
         * @param parameters 包含附加到协定的参数的配置
         * @throws Exception  实现可能会转发由运行时捕获的异常。当运行时捕获异常时，它将中止任务，并允许故障转移逻辑决定是否重试任务执行。
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            //在open方法中获取上一次温度，如果写在方法外面，即在代码块中执行，
            // 则在类加载时执行，此时getRunTimeContext获取不到，是null，所以需要写道open中
            //2.为什么不写到flatMap里，效果一样，但是每一条数据都会调用flatMap方法，而在flatMap中不需要每次都
            //获取state，只需要更新state的值就行了，所以应该写在open中。
            lastTempstate = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<String> out) throws Exception {
            //获取last（解释===>most recent in time; latest）上一次的温度
            Double lastTemp = lastTempstate.value();
            //将当前温度传入lastTempState中
            lastTempstate.update(value.getTemp());
            if (lastTemp != null && Math.abs(value.getTemp()-lastTemp)>10){
                //用out。collect可以自由决定collect什么内容输出，此方法返回值void
                // 而使用map则必须return定义的泛型
                out.collect("温度器温度变化超过"+threshold+"度！");
            }
        }
    }
}
