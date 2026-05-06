# 实验运行说明

本文档用于复现论文第 5 章中的本地实验。所有命令默认在项目根目录执行：

```bash
cd /Users/wenjiehu/graduate/flink-connector-kafka
```

说明：论文中的实验属于本地单机原型验证。实验作业都是长时间运行的 Flink local job，统计到足够数据后需要手动停止进程，通常用 `Ctrl-C` 结束。

## 1. 前置条件

需要本机具备：

- JDK 17 或以上。
- Maven。
- 本地 Kafka broker，监听地址为 `localhost:9092`。
- Kafka CLI 工具，例如 `kafka-topics`、`kafka-get-offsets`。
- Ruby。根目录 `run.sh` 用 Ruby 读取 YAML。

先确认 Kafka 可访问：

```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

构建实验 jar：

```bash
mvn -DskipTests -pl app -am package
```

实验 jar 路径为：

```bash
app/target/dynamic-kafka-sink-app-1.0-SNAPSHOT.jar
```

## 2. 清理与创建 Topic

为避免历史 offset 干扰，建议每轮实验前使用新的 topic 名称，或删除旧 topic 后重建。

可选清理命令：

```bash
kafka-topics --bootstrap-server localhost:9092 --delete --topic exp-wjh-seed
kafka-topics --bootstrap-server localhost:9092 --delete --topic exp-auto-seed
kafka-topics --bootstrap-server localhost:9092 --delete --topic exp-auto-1
kafka-topics --bootstrap-server localhost:9092 --delete --topic exp-auto-2
kafka-topics --bootstrap-server localhost:9092 --delete --topic exp-auto-3
kafka-topics --bootstrap-server localhost:9092 --delete --topic exp-dynamic-c
kafka-topics --bootstrap-server localhost:9092 --delete --topic exp-dynamic-d
kafka-topics --bootstrap-server localhost:9092 --delete --topic exp-dynamic-e
kafka-topics --bootstrap-server localhost:9092 --delete --topic exp-dynamic-f
kafka-topics --bootstrap-server localhost:9092 --delete --topic exp-regular-2
kafka-topics --bootstrap-server localhost:9092 --delete --topic exp-eo-a
kafka-topics --bootstrap-server localhost:9092 --delete --topic exp-eo-b
kafka-topics --bootstrap-server localhost:9092 --delete --topic exp-wjh-eo-seed
```

如果 topic 不存在，删除命令报错可以忽略。删除后等待几秒，再创建实验 topic：

```bash
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-wjh-seed --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-auto-seed --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-dynamic-c --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-dynamic-d --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-dynamic-e --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-dynamic-f --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-regular-2 --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-eo-a --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-eo-b --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-wjh-eo-seed --partitions 1 --replication-factor 1
```

`exp-wjh-seed` 和 `exp-wjh-eo-seed` 是为了匹配 `stream_pattern`，保证动态 Sink 启动时能解析到一个初始 route。

查看 topic offset：

```bash
kafka-get-offsets --bootstrap-server localhost:9092 --topic exp-dynamic-c
```

输出类似：

```text
exp-dynamic-c:0:2812
```

最后一列可作为该 topic 当前写入条数的近似值。若 topic 是新建且没有历史数据，则可直接用于统计。

## 3. 发现实时性实验

目标：验证新增匹配 topic 后，动态 Sink 是否能在不重启作业的情况下发现并开始写入。

终端 1 启动作业：

```bash
java -cp app/target/dynamic-kafka-sink-app-1.0-SNAPSHOT.jar \
  org.apache.flink.dynamic.sink.job.AutoDiscoveryJob \
  --bootstrap-servers localhost:9092 \
  --stream-pattern '^exp-auto-.*$' \
  --cluster-id default-cluster \
  --emit-interval-ms 50 \
  --parallelism 1 \
  --discovery-interval-ms 2000
```

终端 2 依次创建新 topic，并记录创建完成时间：

```bash
date '+%s.%3N'
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-auto-1 --partitions 1 --replication-factor 1
```

随后轮询 offset，直到最后一列大于 0：

```bash
while true; do
  date '+%s.%3N'
  kafka-get-offsets --bootstrap-server localhost:9092 --topic exp-auto-1
  sleep 0.2
done
```

对 `exp-auto-2`、`exp-auto-3` 重复上述步骤。首次写入延迟计算为：

```text
首次观测到 offset > 0 的时间 - topic 创建完成时间
```

论文中的判定标准：最大延迟不超过 3 s。

## 4. 显式 route 与静态基线吞吐实验

目标：验证 `route-a`、`route-b` 是否正确分发到两个物理 topic，并与静态 KafkaSink 基线比较吞吐。

动态 2-route 作业使用 `config.experiment.large.yaml`：

```bash
java -cp app/target/dynamic-kafka-sink-app-1.0-SNAPSHOT.jar \
  org.apache.flink.dynamic.sink.job.DynamicJob \
  --bootstrap-servers localhost:9092 \
  --stream-pattern '^exp-wjh.*$' \
  --cluster-id default-cluster \
  --emit-interval-ms 5 \
  --parallelism 1 \
  --discovery-interval-ms 2000 \
  --config-file config.experiment.large.yaml
```

运行约 30 s 后手动停止。统计两个目标 topic：

```bash
kafka-get-offsets --bootstrap-server localhost:9092 --topic exp-dynamic-c
kafka-get-offsets --bootstrap-server localhost:9092 --topic exp-dynamic-d
```

静态基线作业：

```bash
java -cp app/target/dynamic-kafka-sink-app-1.0-SNAPSHOT.jar \
  org.apache.flink.dynamic.sink.job.RegularJob \
  --bootstrap-servers localhost:9092 \
  --topic exp-regular-2 \
  --emit-interval-ms 5 \
  --parallelism 1
```

运行约 30 s 后手动停止，并统计：

```bash
kafka-get-offsets --bootstrap-server localhost:9092 --topic exp-regular-2
```

吞吐计算：

```text
近似吞吐 = 输出总条数 / 运行秒数
动态 2-route 输出总条数 = exp-dynamic-c offset + exp-dynamic-d offset
吞吐下降比例 = (静态基线吞吐 - 动态吞吐) / 静态基线吞吐
```

论文中的判定标准：动态 2-route 相对静态基线吞吐下降不超过 5%。

## 5. 4-route 小规模扩展性实验

目标：验证 route 数从 2 增加到 4 后，吞吐是否出现明显下降。

使用 `config.experiment.routes4.yaml`：

```bash
java -cp app/target/dynamic-kafka-sink-app-1.0-SNAPSHOT.jar \
  org.apache.flink.dynamic.sink.job.DynamicJob \
  --bootstrap-servers localhost:9092 \
  --stream-pattern '^exp-wjh.*$' \
  --cluster-id default-cluster \
  --emit-interval-ms 5 \
  --parallelism 1 \
  --discovery-interval-ms 2000 \
  --config-file config.experiment.routes4.yaml
```

运行约 30 s 后停止，统计四个 topic：

```bash
kafka-get-offsets --bootstrap-server localhost:9092 --topic exp-dynamic-c
kafka-get-offsets --bootstrap-server localhost:9092 --topic exp-dynamic-d
kafka-get-offsets --bootstrap-server localhost:9092 --topic exp-dynamic-e
kafka-get-offsets --bootstrap-server localhost:9092 --topic exp-dynamic-f
```

如果 `exp-dynamic-c`、`exp-dynamic-d` 复用了前一组实验，需要用本轮结束 offset 减去本轮开始 offset，得到增量条数。

论文中的判定标准：4-route 相对 2-route 吞吐下降不超过 10%。

## 6. Checkpoint 协同实验

目标：验证动态 Sink 的 route 级状态是否能参与 Flink checkpoint。

```bash
java -cp app/target/dynamic-kafka-sink-app-1.0-SNAPSHOT.jar \
  org.apache.flink.dynamic.sink.job.DynamicJob \
  --bootstrap-servers localhost:9092 \
  --stream-pattern '^exp-wjh.*$' \
  --cluster-id default-cluster \
  --emit-interval-ms 5 \
  --parallelism 1 \
  --discovery-interval-ms 2000 \
  --checkpoint-interval-ms 5000 \
  --config-file config.experiment.large.yaml
```

观察控制台日志中的 checkpoint 完成信息。需要记录：

- 成功 checkpoint 次数。
- 每次 checkpoint 的完成时延。
- 是否出现 source、broadcast control stream、sink 共同参与 checkpoint 的日志。

论文中的判定标准：连续完成 checkpoint，且稳态 checkpoint 完成时延低于 100 ms。

## 7. EXACTLY_ONCE 事务路径可运行性实验

目标：验证 route 级 committable 与 committer 在本地小规模 `EXACTLY_ONCE` 模式下可运行。

使用 `config.experiment.exactly_once.yaml`：

```bash
java -cp app/target/dynamic-kafka-sink-app-1.0-SNAPSHOT.jar \
  org.apache.flink.dynamic.sink.job.DynamicJob \
  --bootstrap-servers localhost:9092 \
  --stream-pattern '^exp-wjh-eo.*$' \
  --cluster-id default-cluster \
  --emit-interval-ms 10 \
  --parallelism 1 \
  --discovery-interval-ms 2000 \
  --checkpoint-interval-ms 5000 \
  --delivery-guarantee EXACTLY_ONCE \
  --transactional-id-prefix thesis-eo-demo-$(date +%s) \
  --config-file config.experiment.exactly_once.yaml
```

注意：

- `transactional-id-prefix` 每次运行建议不同，避免和上一次未完全清理的事务前缀冲突。
- 本地 Kafka 需要允许事务相关配置。若出现 transaction timeout 相关错误，需要检查 broker 配置和代码中的 producer property。

运行到至少 5 次 checkpoint 成功后停止。统计两个事务目标 topic：

```bash
kafka-get-offsets --bootstrap-server localhost:9092 --topic exp-eo-a
kafka-get-offsets --bootstrap-server localhost:9092 --topic exp-eo-b
```

论文中的判定标准：至少 5 次 checkpoint 成功，且两个目标 topic 都有 committed 输出。

## 8. 根目录 run.sh 的快捷入口

根目录 `run.sh` 主要服务于默认 `config.yaml`，可用于快速 smoke test：

```bash
./run.sh setup
./run.sh run dynamic
./run.sh run regular
```

如果要复现论文第 5 章中的各组实验，建议使用上文的显式 `java -cp ...` 命令和 `config.experiment*.yaml` 配置文件。

## 9. 记录结果模板

每次实验建议记录：

```text
实验名称：
配置文件：
启动时间：
停止时间：
运行时长：
topic offsets：
输出总条数：
近似吞吐：
checkpoint 次数：
checkpoint 完成时延：
异常日志：
结论：
```

如果实验 topic 不是新建的，务必记录开始 offset 和结束 offset，用增量计算本轮输出条数。
