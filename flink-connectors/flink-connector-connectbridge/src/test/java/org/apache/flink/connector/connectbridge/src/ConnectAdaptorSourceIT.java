package org.apache.flink.connector.connectbridge.src;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.connectbridge.src.reader.deserializer.ConnectRecordDeserializationSchema;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.test.junit5.InjectMiniCluster;

import com.google.common.collect.ImmutableMap;

import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.flink.util.Collector;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ConnectAdaptorSourceIT {


    private static final int PARALLELISM = 4;


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
    public void testBasicRead(@InjectMiniCluster MiniCluster miniCluster)  throws Exception {


       ConnectAdaptorSource<String> source =
                new ConnectAdaptorSourceBuilder(ImmutableMap.of()).setDeserializer(new TestingHackedConnectorDeserializer()).
                        setBounded(Boundedness.BOUNDED).build();

       StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
       DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "testBasicRead");

        executeAndVerify(env, stream);
    }

    private static class TestingHackedConnectorDeserializer
            implements ConnectRecordDeserializationSchema<String> {

        @Override
        public void deserialize(SourceRecord record, Collector<String> out) throws IOException {
            //for now just a hack to get string type
            Object val= record.value();

        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
    private void executeAndVerify(
            StreamExecutionEnvironment env, DataStream<String> stream) throws Exception {
        stream.addSink(
                new RichSinkFunction<String>() {
                    @Override
                    public void open(Configuration parameters) {
                        getRuntimeContext()
                                .addAccumulator("result", new ListAccumulator<String>());
                    }

                    @Override
                    public void invoke(String value, Context context) {
                        getRuntimeContext().getAccumulator("result").add(value);
                    }
                });
        List<String> result = env.execute().getAccumulatorResult("result");
        assertThat(result.size()!=0);
    }


}
