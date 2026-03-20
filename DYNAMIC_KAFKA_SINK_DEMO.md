# Dynamic Kafka Sink Demo

This module provides a minimal runnable Flink job for the new `dynamic-kafka` sink implementation.

## What is included

- `DynamicKafkaSinkDemoJob`: a DataStream job that writes a small string dataset into a
  `DynamicKafkaSink`.
- `docker-compose.yml`: a standalone Kafka environment for manual demo runs.
- `Dockerfile`: a local image definition for the demo job.
- `run.sh`: the single entrypoint for image build, run, and teardown.
- `.env.example`: default environment variables for the demo folder.

## Run locally

From the `dynamic-kafka-sink-demo/` directory:

```bash
cp .env.example .env
./run.sh run
```

This starts Kafka via `docker compose`, rebuilds the local image only when relevant source files
changed, runs that image against the Kafka container network, and then prints the records from the
topic.

## Refresh the local image

```bash
./run.sh push
```

This command rebuilds the local demo image from the latest workspace code and packages the
single-module workspace artifact.

## Stop the demo environment

```bash
./run.sh down
```

## Build the demo jar

If you want the shaded demo artifact:

```bash
mvn package
```
