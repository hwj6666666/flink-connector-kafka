package org.apache.flink.dynamic.sink.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.dynamic.sink.sink.KafkaSinkBuilder;
import org.apache.flink.dynamic.sink.util.ConfigUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/** Benchmark job for the metadata-driven auto-discovery path. */
public class AutoDiscoveryJob {
    private static final String LETTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final int STRING_LENGTH = 8;
    private static final int EIGHT_DIGIT_BOUND = 100_000_000;
    private static final long DEFAULT_INTERVAL_MS = 50L;
    private static final long DEFAULT_DISCOVERY_INTERVAL_MS = 2000L;
    private static final int DEFAULT_PARALLELISM = 1;
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy:MM:dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        Map<String, String> parameters = ConfigUtil.parseArgs(args);
        String bootstrapServers = ConfigUtil.getRequired(parameters, "bootstrap-servers");
        String streamPattern = ConfigUtil.getRequired(parameters, "stream-pattern");
        String clusterId = parameters.getOrDefault("cluster-id", "default-cluster");
        int parallelism =
                Integer.parseInt(
                        parameters.getOrDefault(
                                "parallelism", String.valueOf(DEFAULT_PARALLELISM)));
        long intervalMs =
                Long.parseLong(
                        parameters.getOrDefault(
                                "emit-interval-ms", String.valueOf(DEFAULT_INTERVAL_MS)));
        long discoveryIntervalMs =
                Long.parseLong(
                        parameters.getOrDefault(
                                "discovery-interval-ms",
                                String.valueOf(DEFAULT_DISCOVERY_INTERVAL_MS)));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        env.fromSource(
                        createRandomCsvSource(intervalMs),
                        WatermarkStrategy.noWatermarks(),
                        "auto-discovery-source")
                .sinkTo(
                        new KafkaSinkBuilder()
                                .build(
                                        bootstrapServers,
                                        clusterId,
                                        streamPattern,
                                        discoveryIntervalMs,
                                        DeliveryGuarantee.AT_LEAST_ONCE,
                                        null))
                .name("dynamic-kafka-sink-auto-discovery");

        env.execute("Auto Discovery Benchmark Job");
    }

    private static DataGeneratorSource<DynamicSinkEvent> createRandomCsvSource(long intervalMs) {
        GeneratorFunction<Long, DynamicSinkEvent> generator =
                ignored -> DynamicSinkEvent.data(null, generateRecord(ThreadLocalRandom.current()));
        long recordsPerSecond = intervalMs <= 0 ? Long.MAX_VALUE : Math.max(1L, 1000L / intervalMs);
        return new DataGeneratorSource<>(
                generator,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(recordsPerSecond),
                TypeInformation.of(DynamicSinkEvent.class));
    }

    private static String generateRecord(ThreadLocalRandom random) {
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMATTER);
        int number = random.nextInt(0, EIGHT_DIGIT_BOUND);
        StringBuilder word = new StringBuilder(STRING_LENGTH);
        for (int i = 0; i < STRING_LENGTH; i++) {
            int idx = random.nextInt(LETTERS.length());
            word.append(LETTERS.charAt(idx));
        }
        return timestamp + "," + String.format("%08d", number) + "," + word;
    }
}
