package org.apache.flink.dynamic.sink.job;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class OrderJobSmokeTest {

    @Test
    void testMainClassExists() {
        assertNotNull(OrderJob.class);
    }
}
