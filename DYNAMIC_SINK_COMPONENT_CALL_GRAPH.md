# Dynamic Sink Component Call Graph

This document focuses on component invocation relationships in the current project implementation.

## Component Call Graph

```mermaid
flowchart TB
    subgraph AppModule["app module"]
        A1["DynamicJob"]
        A2["KafkaSinkBuilder (app)"]
        A1 --> A2
    end

    subgraph ConnectorBuild["connector build layer"]
        B1["DynamicKafkaSink.builder()"]
        B2["DynamicKafkaSinkBuilder"]
        B3["StreamPatternSubscriber<br/>or KafkaStreamSetSubscriber"]
        B4["SingleClusterTopicMetadataService<br/>(KafkaMetadataService)"]
        B5["KafkaRecordSerializationSchema"]
        B1 --> B2
        B2 --> B3
        B2 --> B4
        B2 --> B5
    end

    subgraph ConnectorRuntime["connector runtime layer"]
        C1["DynamicKafkaSink"]
        C2["DynamicKafkaSinkWriter"]
        C3["refreshRouteIfNeeded()"]
        C4["resolveRoutes()"]
        C5["activeRouteIds"]
        C6["clusterWritersByRouteId"]
        C1 --> C2
        C2 --> C3
        C3 --> C4
        C4 --> C5
        C4 --> C6
    end

    subgraph MetadataAndDiscovery["metadata and discovery"]
        D1["KafkaStreamSubscriber.getSubscribedStreams()"]
        D2["KafkaMetadataService.getAllStreams()"]
        D3["AdminClient.listTopics()"]
        D1 --> D2
        D2 --> D3
    end

    subgraph PerRouteSink["per-route underlying sink"]
        E1["createClusterWriter(routeId)"]
        E2["KafkaSink.builder()"]
        E3["PrecommittingStatefulSinkWriter"]
        E4["Kafka topic(s)"]
        E1 --> E2
        E2 --> E3
        E3 --> E4
    end

    subgraph StateAndCommit["state and commit path"]
        F1["snapshotState()"]
        F2["prepareCommit()"]
        F3["DynamicKafkaSinkWriterState"]
        F4["DynamicKafkaCommittable"]
        F5["DynamicKafkaCommitter"]
        F1 --> F3
        F2 --> F4
        F4 --> F5
    end

    A2 --> B1
    B2 --> C1
    C3 --> D1
    C6 --> E1
    C2 --> F1
    C2 --> F2
```

## How to Read This Graph

- `DynamicJob` in `app` builds the sink through the app-level `KafkaSinkBuilder`.
- `DynamicKafkaSinkBuilder` wires subscriber, metadata service, serializer, and creates `DynamicKafkaSink`.
- `DynamicKafkaSinkWriter` is the runtime core:
  - refreshes metadata by interval,
  - resolves available routes,
  - writes each record to all currently active routes.
- Each route gets an internal regular `KafkaSink` writer instance.
- Checkpoint/commit is managed via `DynamicKafkaSinkWriterState` and `DynamicKafkaCommittable`.
