package org.apache.flink.connector.kafka.sink;

import java.util.Collections;
import java.util.Map;

/**
 * Optional contract for sink input records that carry route updates and id-based route selection.
 *
 * <p>For route updates, a map entry with a {@code null} destination indicates that the logical
 * route should be removed.
 */
public interface DynamicKafkaRouteEvent {

    default boolean isRouteUpdate() {
        return false;
    }

    default Map<String, KafkaRouteDestination> getRouteUpdates() {
        return Collections.emptyMap();
    }

    default String getRouteId() {
        return null;
    }
}
