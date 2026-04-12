package org.apache.flink.dynamic.sink.job;

import org.apache.flink.connector.kafka.sink.DynamicKafkaRouteEvent;
import org.apache.flink.connector.kafka.sink.KafkaRouteDestination;

import java.util.Collections;
import java.util.Map;

/** Unified stream event for id->destination updates and <id,message> data records. */
public class DynamicSinkEvent implements DynamicKafkaRouteEvent {
    private final boolean routeUpdate;
    private final String routeId;
    private final String message;
    private final Map<String, KafkaRouteDestination> routeUpdates;

    private DynamicSinkEvent(
            boolean routeUpdate,
            String routeId,
            String message,
            Map<String, KafkaRouteDestination> routeUpdates) {
        this.routeUpdate = routeUpdate;
        this.routeId = routeId;
        this.message = message;
        this.routeUpdates = routeUpdates == null ? Collections.emptyMap() : routeUpdates;
    }

    public static DynamicSinkEvent data(String routeId, String message) {
        return new DynamicSinkEvent(false, routeId, message, Collections.emptyMap());
    }

    public static DynamicSinkEvent update(Map<String, KafkaRouteDestination> routeUpdates) {
        return new DynamicSinkEvent(true, null, null, routeUpdates);
    }

    public static DynamicSinkEvent remove(String routeId) {
        return new DynamicSinkEvent(
                true, null, null, Collections.singletonMap(routeId, (KafkaRouteDestination) null));
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean isRouteUpdate() {
        return routeUpdate;
    }

    @Override
    public Map<String, KafkaRouteDestination> getRouteUpdates() {
        return routeUpdates;
    }

    @Override
    public String getRouteId() {
        return routeId;
    }
}
