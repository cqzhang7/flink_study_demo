package com.zcq2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author cqzhang
 * @create 2020-12-12 0:21
 */
public class Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDataStream = env.socketTextStream("hadoop102", 9999);
        FlinkJedisPoolConfig jedispoolconfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102")
                .setPort(6379).build();
        socketDataStream.addSink(new RedisSink<>(jedispoolconfig,new MyRedisMapper()));

        env.execute("");
    }
    public static class MyRedisMapper implements RedisMapper<String>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"Sensor");
        }

        @Override
        public String getKeyFromData(String data) {
            return data.split(",")[0];
        }

        @Override
        public String getValueFromData(String data) {
            return data.split(",")[2];
        }


    }
}
