# syntax=docker/dockerfile:1.7

FROM maven:3.9.9-eclipse-temurin-17 AS builder

WORKDIR /workspace

ARG MVN_BUILD_ARGS="-Dmaven.test.skip=true -Dcheckstyle.skip=true -Dspotless.skip=true -Denforcer.skip=true -Drat.skip=true -Djapicmp.skip=true -Dmdep.analyze.skip=true"

COPY pom.xml pom.xml

RUN --mount=type=cache,target=/root/.m2 \
    mvn -N ${MVN_BUILD_ARGS} dependency:go-offline

RUN --mount=type=cache,target=/root/.m2 \
    mvn ${MVN_BUILD_ARGS} -DincludeScope=compile dependency:go-offline

COPY src src

RUN --mount=type=cache,target=/root/.m2 \
    mvn ${MVN_BUILD_ARGS} -Dmaven.test.skip=true package

FROM eclipse-temurin:17-jre

WORKDIR /opt/dynamic-kafka-sink-demo

COPY --from=builder /workspace/target/flink-connector-kafka-parent-5.0-SNAPSHOT.jar app.jar

ENTRYPOINT ["java", "-jar", "/opt/dynamic-kafka-sink-demo/app.jar"]
