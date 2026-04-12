package org.apache.flink.dynamic.sink.job;

import org.apache.flink.dynamic.sink.sink.KafkaSinkBuilder;
import org.apache.flink.dynamic.sink.util.ConfigUtil;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRouteDestination;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/** Flink benchmark job for DynamicKafkaSink. */
public class DynamicJob {
    private static final String LETTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final int STRING_LENGTH = 8;
    private static final int EIGHT_DIGIT_BOUND = 100_000_000;
    private static final long DEFAULT_INTERVAL_MS = 0L;
    private static final long DEFAULT_DISCOVERY_INTERVAL_MS = 2000L;
    private static final long DEFAULT_CHECKPOINT_INTERVAL_MS = 0L;
    private static final int DEFAULT_PARALLELISM = 1;
    private static final String DEFAULT_CONFIG_FILE = "config.yaml";
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy:MM:dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        Map<String, String> parameters = ConfigUtil.parseArgs(args);
        String bootstrapServers = ConfigUtil.getRequired(parameters, "bootstrap-servers");
        String streamPattern = ConfigUtil.getRequired(parameters, "stream-pattern");
        String clusterId = parameters.getOrDefault("cluster-id", "default-cluster");
        String configFile = parameters.getOrDefault("config-file", DEFAULT_CONFIG_FILE);
        int parallelism =
                Integer.parseInt(parameters.getOrDefault("parallelism", String.valueOf(DEFAULT_PARALLELISM)));
        long intervalMs =
                Long.parseLong(parameters.getOrDefault("emit-interval-ms", String.valueOf(DEFAULT_INTERVAL_MS)));
        long discoveryIntervalMs =
                Long.parseLong(
                        parameters.getOrDefault(
                                "discovery-interval-ms",
                                String.valueOf(DEFAULT_DISCOVERY_INTERVAL_MS)));
        long checkpointIntervalMs =
                Long.parseLong(
                        parameters.getOrDefault(
                                "checkpoint-interval-ms",
                                String.valueOf(DEFAULT_CHECKPOINT_INTERVAL_MS)));
        DeliveryGuarantee deliveryGuarantee =
                DeliveryGuarantee.valueOf(
                        parameters
                                .getOrDefault("delivery-guarantee", DeliveryGuarantee.AT_LEAST_ONCE.name())
                                .toUpperCase());
        String transactionalIdPrefix = parameters.get("transactional-id-prefix");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        if (checkpointIntervalMs > 0) {
            env.enableCheckpointing(checkpointIntervalMs);
        }

        Map<String, KafkaRouteDestination> routeMap =
                loadRouteMapFromYaml(configFile, clusterId, bootstrapServers);
        List<String> routeIds = new ArrayList<>(routeMap.keySet());
        if (routeIds.isEmpty()) {
            throw new IllegalStateException("No dynamic.route_map entries found in " + configFile);
        }

        MapStateDescriptor<String, KafkaRouteDestination> routeMapStateDesc =
                new MapStateDescriptor<>(
                        "id-to-kafka-route",
                        TypeInformation.of(String.class),
                        TypeInformation.of(KafkaRouteDestination.class));

        BroadcastStream<DynamicSinkEvent> routeUpdates =
                env.fromData(
                                DynamicSinkEvent.update(routeMap))
                        .broadcast(routeMapStateDesc);

        env.fromSource(
                        createRandomCsvSource(intervalMs, routeIds),
                        WatermarkStrategy.noWatermarks(),
                        "id-message-source")
                .connect(routeUpdates)
                .process(new RouteBroadcastProcessFunction(routeMapStateDesc))
                .sinkTo(
                        new KafkaSinkBuilder()
                                .build(
                                        bootstrapServers,
                                        clusterId,
                                        streamPattern,
                                        discoveryIntervalMs,
                                        deliveryGuarantee,
                                        transactionalIdPrefix))
                .name("dynamic-kafka-sink-id-routing");

        env.execute("Dynamic Sink Benchmark Job");
    }

    private static DataGeneratorSource<DynamicSinkEvent> createRandomCsvSource(
            long intervalMs, List<String> routeIds) {
        GeneratorFunction<Long, DynamicSinkEvent> generator =
                ignored -> {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    String routeId = routeIds.get(random.nextInt(routeIds.size()));
                    return DynamicSinkEvent.data(routeId, generateRecord(random));
                };
        long recordsPerSecond = intervalMs <= 0 ? Long.MAX_VALUE : Math.max(1L, 1000L / intervalMs);
        return new DataGeneratorSource<>(
                generator,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(recordsPerSecond),
                TypeInformation.of(DynamicSinkEvent.class));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, KafkaRouteDestination> loadRouteMapFromYaml(
            String configFile, String defaultClusterId, String defaultBootstrapServers) throws Exception {
        Map<String, KafkaRouteDestination> routeMap = new LinkedHashMap<>();
        Load loader = new Load(LoadSettings.builder().build());
        try (InputStream input = new FileInputStream(configFile)) {
            Object loaded = loader.loadFromInputStream(input);
            if (!(loaded instanceof Map)) {
                return routeMap;
            }
            Map<String, Object> root = (Map<String, Object>) loaded;
            Object dynamicObj = root.get("dynamic");
            if (!(dynamicObj instanceof Map)) {
                return routeMap;
            }
            Map<String, Object> dynamic = (Map<String, Object>) dynamicObj;
            Object routeMapObj = dynamic.get("route_map");
            if (!(routeMapObj instanceof Map)) {
                return routeMap;
            }
            Map<String, Object> rawRouteMap = (Map<String, Object>) routeMapObj;
            for (Map.Entry<String, Object> entry : rawRouteMap.entrySet()) {
                String routeId = entry.getKey();
                if (routeId == null || routeId.isBlank() || !(entry.getValue() instanceof Map)) {
                    continue;
                }
                Map<String, Object> routeCfg = (Map<String, Object>) entry.getValue();
                String clusterId =
                        stringOrDefault(routeCfg.get("cluster_id"), defaultClusterId);
                String bootstrapServers =
                        stringOrDefault(routeCfg.get("bootstrap_servers"), defaultBootstrapServers);
                String topicRegex = stringOrDefault(routeCfg.get("topic_regex"), null);
                if (topicRegex == null || topicRegex.isBlank()) {
                    continue;
                }
                routeMap.put(
                        routeId,
                        new KafkaRouteDestination(clusterId, bootstrapServers, topicRegex));
            }
        }
        return routeMap;
    }

    private static String stringOrDefault(Object value, String defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        String text = String.valueOf(value);
        return text.isBlank() ? defaultValue : text;
    }

    private static String generateRecord(ThreadLocalRandom random) {
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMATTER);
        int number = random.nextInt(0, EIGHT_DIGIT_BOUND);
        StringBuilder word = new StringBuilder(STRING_LENGTH);
        for (int j = 0; j < STRING_LENGTH; j++) {
            int idx = random.nextInt(LETTERS.length());
            word.append(LETTERS.charAt(idx));
        }
        return timestamp + "," + String.format("%08d", number) + "," + word;
    }

    private static final class RouteBroadcastProcessFunction
            extends BroadcastProcessFunction<DynamicSinkEvent, DynamicSinkEvent, DynamicSinkEvent> {
        private final MapStateDescriptor<String, KafkaRouteDestination> routeMapStateDesc;

        private RouteBroadcastProcessFunction(
                MapStateDescriptor<String, KafkaRouteDestination> routeMapStateDesc) {
            this.routeMapStateDesc = routeMapStateDesc;
        }

        @Override
        public void processElement(
                DynamicSinkEvent value, ReadOnlyContext ctx, Collector<DynamicSinkEvent> out)
                throws Exception {
            if (value == null || value.isRouteUpdate() || value.getRouteId() == null) {
                return;
            }
            if (ctx.getBroadcastState(routeMapStateDesc).contains(value.getRouteId())) {
                out.collect(value);
            }
        }

        @Override
        public void processBroadcastElement(
                DynamicSinkEvent value, Context ctx, Collector<DynamicSinkEvent> out)
                throws Exception {
            if (value == null || !value.isRouteUpdate()) {
                return;
            }
            for (Map.Entry<String, KafkaRouteDestination> entry : value.getRouteUpdates().entrySet()) {
                if (entry.getValue() == null) {
                    ctx.getBroadcastState(routeMapStateDesc).remove(entry.getKey());
                } else {
                    ctx.getBroadcastState(routeMapStateDesc).put(entry.getKey(), entry.getValue());
                }
            }
            out.collect(value);
        }
    }
}
