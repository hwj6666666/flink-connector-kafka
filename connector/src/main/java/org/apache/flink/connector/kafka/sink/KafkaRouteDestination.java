package org.apache.flink.connector.kafka.sink;

import java.io.Serializable;
import java.util.Objects;

/** Destination descriptor used by id-based dynamic routing. */
public class KafkaRouteDestination implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String kafkaClusterId;
    private final String bootstrapServers;
    private final String topicPattern;

    public KafkaRouteDestination(String kafkaClusterId, String bootstrapServers, String topicPattern) {
        this.kafkaClusterId = kafkaClusterId;
        this.bootstrapServers = bootstrapServers;
        this.topicPattern = topicPattern;
    }

    public String getKafkaClusterId() {
        return kafkaClusterId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopicPattern() {
        return topicPattern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaRouteDestination)) {
            return false;
        }
        KafkaRouteDestination that = (KafkaRouteDestination) o;
        return Objects.equals(kafkaClusterId, that.kafkaClusterId)
                && Objects.equals(bootstrapServers, that.bootstrapServers)
                && Objects.equals(topicPattern, that.topicPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kafkaClusterId, bootstrapServers, topicPattern);
    }
}
