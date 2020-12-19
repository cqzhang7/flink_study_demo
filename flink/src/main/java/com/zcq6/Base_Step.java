package com.zcq6;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @author cqzhang
 * @create 2020-12-16 18:32
 */
public class Base_Step {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 1 创建表环境---通过StreamTableEnvironment
        // see "Create a TableEnvironment" section
        // create a TableEnvironment for specific planner batch or streaming
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2  create a Table  &&  register an output Table
        tableEnv.connect(new FileSystem().path("flink/sensor"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");

        //tableEnv.connect().createTemporaryTable("outputTable");
        //TODO 3
        // 3.1 create a Table object from a Table API query
        // 3.2 create a Table object from a SQL query
        Table tableRes = tableEnv.from("inputTable").select("id,ts");
        Table sqlRes = tableEnv.sqlQuery("select id,temp from inputTable");

        //TODO 4 结果表写人输出表
        // emit a Table API result Table to a TableSink, same for SQL result

        //此处我们转换为追加流并打印
        tableEnv.toAppendStream(tableRes, Row.class).print("table:");
        tableEnv.toAppendStream(sqlRes,Row.class).print("sql:");

        env.execute("");
    }
}
