package org.apache.flink.connector.connectbridge.table;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.flink.test.util.SuccessException;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ConnectorAdaptorTableITCase extends AbstractTestBase {
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;
    private static String format = "json";

    // Timer for scheduling logging task if the test hangs


    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    }

    @Before
    public void before() {
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);
    }

    @Test
    public void testConnectorSource() throws Exception {

        final String createTable =
                String.format(
                        "create table datagen (\n"
                                + "  op STRING\n"
                                + ") with (\n"
                                + "  'connector' = '%s',\n"
                                + "  %s\n"
                                + ")",
                        ConnectAdaptorTableFactory.IDENTIFIER,
                        formatOptions());

        tEnv.executeSql(createTable);
        // ---------- Consume stream from Kafka -------------------

        String query =
                "SELECT\n"
                        + "  op\n"
                        + "FROM datagen\n";

        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(2);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("Job-2");
        } catch (Throwable e) {
            if (!isCausedByJobFinished(e)) {
                // re-throw
                throw e;
            }
        }

    }
    private static final class TestingSinkFunction implements SinkFunction<RowData> {

        private static final long serialVersionUID = 455430015321124493L;
        private static List<String> rows = new ArrayList<>();

        private final int expectedSize;

        private TestingSinkFunction(int expectedSize) {
            this.expectedSize = expectedSize;
            rows.clear();
        }

        @Override
        public void invoke(RowData value, Context context) {
            rows.add(value.toString());
            if (rows.size() >= expectedSize) {
                // job finish
                throw new SuccessException();
            }
        }
    }
    private String formatOptions() {
        return String.format("'format' = '%s'", format);
    }

    private static boolean isCausedByJobFinished(Throwable e) {
        if (e instanceof SuccessException) {
            return true;
        } else if (e.getCause() != null) {
            return isCausedByJobFinished(e.getCause());
        } else {
            return false;
        }
    }

}
