package org.apache.flink.connector.connectbridge.src;

import com.google.common.collect.ImmutableMap;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.InjectMiniCluster;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class KafkaConnectAdaptorSourceIT {

    @Test
    private void testBasicRead(@InjectMiniCluster MiniCluster miniCluster)  throws Exception {
       // KafkaConnectAdaptorSource<String> source =
       //         new KafkaConnectAdaptorSourceBuilder<>(ImmutableMap.of()).
       //                 setBounded(Boundedness.BOUNDED).build();

      //  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      //  env.setParallelism(1);
      // // DataStream<PartitionAndValue> stream =
       //         env.fromSource(source, WatermarkStrategy.noWatermarks(), "testBasicRead");
       // executeAndVerify(env, stream);
    }

}
