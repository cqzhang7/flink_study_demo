package com.zcq;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author cqzhang
 * @create 2020-12-09 20:09
 */
public class Flink03_WordCount_UnBounded {

    public static void main(String[] args) throws Exception {
        //1.创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取socket数据流 parameterTool工具包实现动态传参 Run-->edit Configurations
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        DataStreamSource<String> socketDSS = env.socketTextStream("hadoop102", 9999);
        //3.做压平处理
        //SingleOutputStreamOperator可以用其父类DataStream接收，但是如果要调用子类特有的方法则只能用子类接收
        //在Java中称之为开闭原则,
        //    开闭原则
        //    定义: 一个软件实体如类、模块和函数应该对扩展开放,对修改关闭。
        //    用抽象构建框架,用实现扩展细节
        //    优点:提高软件系统的可复用性及可维护性。
        //
        //当代码需要额外扩展或者修改定制专有的功能时，应该提供一种抽象来扩展功能 而不是修改原代码。
        // 然后使用子类的子类即该类的类型接受即可使用该实现类独有的方法
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = socketDSS.flatMap(new Flink01_WordCount_Batch.MyFlatMapper());
        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = tuple2SingleOutputStreamOperator.keyBy(0);
        //5.聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2TupleKeyedStream.sum(1);
        //6.打印输出
        sum.print();
        //7.启动任务
        env.execute("Flink03_WordCount_UnBounded");
    }

}
