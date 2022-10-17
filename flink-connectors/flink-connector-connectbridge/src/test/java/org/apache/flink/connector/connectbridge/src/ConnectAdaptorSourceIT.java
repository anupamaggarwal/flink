package org.apache.flink.connector.connectbridge.src;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.connectbridge.src.reader.deserializer.ConnectValueOnlyDeserializationSchemaWrapper;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConnectAdaptorSourceIT {


    private static final int PARALLELISM = 4;

    private static Map<String, String> DATAGEN_CONFIG = ImmutableMap.<String, String>builder()
            .put("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector")
            .put("connector.task.class", "io.confluent.kafka.connect.datagen.DatagenTask")
            .put("key.converter", "org.apache.kafka.connect.storage.StringConverter")
            .put("value.converter", "org.apache.kafka.connect.json.JsonConverter")
            .put("max.interval", "1000")
            .put("kafka.topic", "Foobar")
            .put("quickstart", "users")
            .put("iterations", "10")
            .put("tasks.max", "1")
            .build();


    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());


    @Test
    public void testBasicRead(@InjectMiniCluster MiniCluster miniCluster) throws Exception {
        ConnectAdaptorSource<String> source =
                new ConnectAdaptorSourceBuilder<String>(DATAGEN_CONFIG).setDeserializer(new ConnectValueOnlyDeserializationSchemaWrapper<>(
                                StringDeserializer.class,DATAGEN_CONFIG.get("value.converter"))).
                        setBounded(Boundedness.BOUNDED).build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(DATAGEN_CONFIG.get("tasks.max")));
        DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "testBasicRead");

        int iterationsRequested = Integer.parseInt(DATAGEN_CONFIG.get("iterations"));
        executeAndVerify(env, stream, iterationsRequested);
    }


    private <T> void executeAndVerify(
            StreamExecutionEnvironment env, DataStream<T> stream, int iterationsRequested
    ) throws Exception {
        stream.addSink(
                new RichSinkFunction<T>() {
                    @Override
                    public void open(Configuration parameters) {
                        getRuntimeContext()
                                .addAccumulator("result", new ListAccumulator<T>());
                    }

                    @Override
                    public void invoke(T value, Context context) {
                        getRuntimeContext().getAccumulator("result").add(value);
                    }
                });
        List<T> result = env.execute().getAccumulatorResult("result");
        System.out.println("Printing values obtained from connector");
        result.forEach(System.out::println);
        assertEquals(result.size(), iterationsRequested);
    }


}
