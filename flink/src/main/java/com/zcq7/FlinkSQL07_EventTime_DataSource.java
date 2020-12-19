package com.zcq7;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;

/**
 * @author cqzhang
 * @create 2020-12-18 20:40
 */
public class FlinkSQL07_EventTime_DataSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        // register the table source
        tEnv.connect(new FileSystem().path("flink/sensor"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("rt", DataTypes.BIGINT()).rowtime(new Rowtime()
                                .timestampsFromField("ts")    // 从字段中提取时间戳
                                .watermarksPeriodicBounded(1000)    // watermark延迟1秒
                        )
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("sensorTable");

        Table sensorTable = tEnv.from("sensorTable");
        sensorTable.printSchema();
    }
}
