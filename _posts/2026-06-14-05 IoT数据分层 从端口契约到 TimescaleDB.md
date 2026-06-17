---
title: 05 IoT 数据分层：从端口契约到 TimescaleDB
date: 2026-06-14 16:00:00 +0800
categories: [用DDD重构IoT后端系列]
tags: [IoT, 领域驱动设计, TimescaleDB, 数据分层, 时序数据, 幂等键]
---

这篇讲"遥测指标存到哪里、怎么查"，围绕 telemetry 上下文里的两个核心存储抽象`TelemetryHotDataPort` 和 `TelemetrySnapshot`，把 IoT 平台的数据分层体系顺着讲一遍：**业务库、时序库、数据湖、数仓各自该装什么**。

> 项目地址：[https://github.com/Liyuwen85/iot-alarm-copilot](https://github.com/Liyuwen85/iot-alarm-copilot){:target="_blank"}

## 目录
> IoT数据的几种形态
>
> 这些存储分别该装什么
>
> 项目里 hot 这一层：TelemetryHotDataPort + TimescaleDB
>
> 双库并写
>
> 还没做的一些事请

## IoT数据的几种形态

像电商类业务系统里，数据形态比较单一，订单、用户、商品，绝大部分是"实体的当前状态 + 少量变更历史"，一个 OLTP 就能扛。IoT不一样，同样是"温度"这个数据，平台里至少要回答四类问题：

| 问题 | 例子 | 数据形态 |
|---|---|---|
| 这台设备**现在**多少度？ | 监控大屏 | 当前状态（每设备一行） |
| 这台设备**过去一天**温度走势？ | 故障复盘 | 时间序列（按时间排列的事件流） |
| **某个型号**的设备**这一周**平均温度？ | 业务报表 | 跨维度聚合（按设备类型、地域汇总） |
| 半年前那次事故**当天**所有设备的原始数据？ | 合规追溯 | 历史明细（冷归档） |

四个问题对应四种**完全不同的查询模式**：

- **当前状态查询**，按 deviceId 主键命中，要求毫秒级响应，并发量取决于在线监控数；
- **时间序列查询**，按时间范围 + deviceId 扫一段，要求按时间分区高效读取；
- **跨维度聚合查询**，多表 join + group by + 复杂聚合，可以慢但要灵活；
- **历史明细查询**，查得不频繁，但单次扫描数据量极大。

用一种存储扛全部的四类查询，基本不可能（IotDB可以搞定时序和历史；但涉及大量上层业务聚合时，还是要独立数仓来处理）。所以 IoT 平台天然要做数据分层，让每种查询落到最适合的存储上。

## 这些存储分别该装什么

工业界一般把 IoT 数据栈分成 hot / warm / cold 三层（也有叫 hot / cold 两层、再独立拉一个数仓的，本质相同）：

| 层级 | 典型存储 | 装什么数据 | 查询模式 |
|---|---|---|---|
| **业务库** | PostgreSQL / MySQL | 设备主数据、当前状态、配置 | 主键查、低延迟、事务 |
| **时序库** | TimescaleDB / InfluxDB / TDengine | 最近 N 天的遥测事件流 | 按时间范围扫、按设备 + 时间扫 |
| **数据湖** | 对象存储 + Parquet / Iceberg | 全量历史明细、原始上报 | 偶尔扫、按列存压缩、批查询 |
| **数仓** | ClickHouse / Doris / 数据中台 | 预聚合的指标、报表数据 | 跨维度聚合、报表、BI |

四种存储各管一段，互不重叠：

- **业务库**，OLTP 数据库的本职：事务一致性、强一致查询、低延迟。设备主数据（device 上下文）、产品模型（也是 device）、规则定义、告警状态都该在这里；
- **时序库**，append-only 的时序事件流，按时间天然分区。**写入吞吐 + 时间范围查询**是它的强项，本项目的遥测事件流就在这一层；
- **数据湖**，历史明细的归档地。冷数据从时序库里"沉"下来后存这里，按列压缩，查询不频繁但保留完整。配合 Parquet / Iceberg 这类格式，可以被 Flink / Spark 直接查；
- **数仓**，业务视角的预聚合层。"昨日各型号设备平均故障率"这种问题，从时序库现算太慢，需要预先按维度算好物化在数仓里。

注意一个常被忽略的事实——**时序库不是数仓**。时序库擅长按时间扫，但跨维度聚合（按设备型号、按地域、按客户）会很慢；数仓反过来，按时间扫不一定快，但跨维度聚合是它的本职。两者是互补关系，不是替代关系。

## 项目里 hot 这一层：TelemetryHotDataPort + TimescaleDB

先看 telemetry 上下文怎么把"时序数据写哪、查哪"抽出来。application 层定义了一个端口：

```java
public interface TelemetryHotDataPort {
    void append(TelemetryEvent event);
    List<TelemetryEventVO> recent(int limit);
}
```

注意端口的命名，它叫 `HotDataPort`，不叫 `TelemetryEventRepository`：

- `Repository` 暗示"持久化领域聚合"，倾向于传统 OLTP 数据库；
- `HotDataPort` 暗示"热数据的存储抽象"，为以后的分层存储留了余地。

从端口名字就可以看出来，一旦未来要把"超过 N 天的数据沉到 warm 层"，业务代码不用改，只需要把这个端口的实现替换成"hot + warm 双写"或者加一个独立的 `WarmDataPort`就可以完成。

### 两个并存的实现

`TelemetryHotDataPort` 在项目里有两个实现：

```java
// 默认实现：PostgreSQL（开发期 fallback，测试也方便）
@Component
@ConditionalOnMissingBean(TelemetryHotDataPort.class)
public class MybatisTelemetryHotDataPort implements TelemetryHotDataPort { ... }

// 生产实现：TimescaleDB（开关启用）
@Component
@ConditionalOnProperty(prefix = "iot.timeseries", name = "enabled", havingValue = "true")
public class TimescaleTelemetryHotDataPort implements TelemetryHotDataPort { ... }
```

`@ConditionalOnProperty` 启用 Timescale 时自动生效，`@ConditionalOnMissingBean` 让普通 Mybatis 实现成为兜底。

### TimescaleDB 实现里的几个工程决策

**1. 独立 DataSource**

业务库和时序库物理隔离，时序库单独建了一个 `HikariDataSource`数据源，跟主数据源完全独立：

```java
@Bean(destroyMethod = "close")
TimescaleTelemetryResources telemetryTimeseriesResources(TelemetryTimeseriesProperties properties) {
    HikariDataSource dataSource = new HikariDataSource();
    dataSource.setDriverClassName(properties.getDriverClassName());
    dataSource.setJdbcUrl(properties.getUrl());
    dataSource.setUsername(properties.getUsername());
    dataSource.setPassword(properties.getPassword());
    dataSource.setPoolName("telemetry-timeseries-pool");
    dataSource.setMaximumPoolSize(4);
    dataSource.setMinimumIdle(1);
    return new TimescaleTelemetryResources(dataSource);
}
```

主要为了开发方便都用 PostgreSQL（Timescale 本身就是 PG 的扩展），生产环境中也可以拆到不同实例、不同机器。

**2. hypertable 用 reportedAt 作为分区键**

```sql
SELECT create_hypertable('telemetry_point', 'reported_at', if_not_exists => TRUE)
```

TimescaleDB 的核心优化是**按时间自动分区**，hypertable 把一张大表切成无数个按时间分布的小 chunk，查询某个时间段时只扫相关 chunk。

注意分区键选的是 `reported_at`（设备上报时间），不是 `created_at`（系统入库时间）。如果用 `created_at` 分区，遇到设备掉线后批量补传数据，所有补传数据会全部落到"今天"的分区，数据就不对了。

**3. 幂等键**

```sql
INSERT INTO telemetry_point (...)
ON CONFLICT (telemetry_event_id, reported_at) DO NOTHING
```

`telemetry_event_id` 是 SHA256 算出的确定性 ID，加上 `reported_at` 形成复合幂等键。这条对应第之前里讲过的"幂等是领域概念"，同一条遥测无论被重投几次，时序库里只有一行。

**4. 索引设计**

```sql
CREATE INDEX idx_telemetry_point_device_reported_at
    ON telemetry_point (device_id, reported_at DESC);
CREATE INDEX idx_telemetry_point_reported_at
    ON telemetry_point (reported_at DESC);
```

两个索引对应两类查询：
- "某设备过去一段时间的数据"，走 `(device_id, reported_at DESC)` 复合索引
- "全局最近的 N 条数据"，走 `(reported_at DESC)` 单索引

时序数据的查询有局限，要么按设备查时间序列，要么按时间扫全局。索引就建这两类，多了反而拖慢写入。

## 双库并写

时序库扛住了"时间序列查询"，但还有一类高频查询时序库扛不住，**当前状态查询**："这台设备现在多少度？"

这种查询有两个特征：

- **按 deviceId 主键命中**，只查一行
- **频率极高**，监控大屏 / 移动端 App 每秒都在刷

如果直接打到时序库，意味着每次都要 `SELECT ... ORDER BY reported_at DESC LIMIT 1`，即便有索引也要走一段扫描。**对每秒上万次的当前状态查询来说，这是浪费**。

本项目的解法是在 telemetry 上下文里做一个独立聚合 `TelemetrySnapshot`，每设备一行，落在业务库里：

```java
public record TelemetrySnapshot(
        DeviceId deviceId,
        Long lastTelemetryEventId,
        TelemetryMetrics metrics,
        Instant lastReportedAt,
        String lastRawJson) { ... }
```

每条遥测进来时，时序库 append 一条事件，同时业务库也 upsert 一条快照：

```java
// TelemetryIngestApplicationService.record(...)
telemetryHotDataPort.append(event);                              // 写时序库

TelemetrySnapshot snapshot = telemetrySnapshotRepository.findByDeviceId(event.deviceId())
        .map(existing -> existing.refreshBy(event))
        .orElseGet(() -> TelemetrySnapshot.capture(event));
telemetrySnapshotRepository.save(snapshot);                       // 写业务库
```

### Snapshot 是独立聚合

这里看 `refreshBy(event)` 的实现，它不是简单覆盖最新值，而是做了很多规则判定：

```java
public TelemetrySnapshot refreshBy(TelemetryEvent event) {
    if (!deviceId.equals(event.deviceId())) {
        throw new BaseDomainException("Telemetry snapshot device mismatch");
    }
    // 乱序到达：旧事件直接丢弃
    if (event.reportedAt().isBefore(lastReportedAt)) {
        return this;
    }
    // 同时刻到达：取 id 较大的那条
    Long nextEventId = event.reportedAt().equals(lastReportedAt)
            ? Math.max(lastTelemetryEventId, event.id())
            : event.id();
    return new TelemetrySnapshot(
            deviceId,
            nextEventId,
            mergeMetrics(event.metrics()),  // 指标合并，不是覆盖
            event.reportedAt().isAfter(lastReportedAt) ? event.reportedAt() : lastReportedAt,
            event.rawJson());
}
```

可查看 [TelemetrySnapshot.java 源码](https://github.com/Liyuwen85/iot-alarm-copilot/blob/main/backend/iot-context-telemetry/src/main/java/com/example/iotalarmcopilot/telemetry/domain/TelemetrySnapshot.java){:target="_blank"}

这一段处理了三个真实场景：

- **乱序到达**，设备掉线后批量补传，旧时间戳的数据不能覆盖新数据，否则当前状态会倒退；
- **同时刻到达**，两条事件 `reportedAt` 相同时，按 telemetryEventId 决定，保证幂等；
- **指标合并**，温度和湿度可能在两次上报里分别到达，新事件不应该把另一个指标"覆盖成 null"，而要 merge。

## 还没做的一些事请

### 1. 冷归档没做

时序库扛不住"全量历史"，一台设备每秒一条数据，一年就是 3000 万条，几万台设备就是千亿级。**hot 层不可能保留全量历史**。

正确的做法是定期把超过阈值（比如 90 天）的数据归档到对象存储 + Parquet 格式，业务库 / 时序库只保留近期数据。归档之后还能用 Trino / Spark / Flink 查冷数据，做合规追溯和大规模回溯分析。

为什么没做？两个原因：

- **数据量没到**，学习型项目，点到即止、核心突出即可；
- **冷归档的工程复杂度集中在数据治理，不在领域建模**，归档策略、双活校验、回查 API、生命周期合规，每一项都要专门工程投入。

以后要做，可以按"`HotDataPort` + `WarmDataPort` 双写、`ArchiveJob` 定期搬运"的模式补上即可完成。

### 2. 降采样没做

TimescaleDB 有一个特性叫 **continuous aggregates**，可以预先按时间窗口（1 分钟、1 小时、1 天）算好聚合（avg、max、min），查询历史趋势时直接读聚合表，速度比扫原始数据快几个量级。

本项目目前所有查询都直接扫 `telemetry_point` 原始表。等真要做“过去 30 天每小时平均温度”这种查询时，再加 continuous aggregates 即可。

### 3. 多租户分区没做

生产环境的 IoT 平台通常是多租户的，不同客户的数据要逻辑或物理隔离。TimescaleDB 的 hypertable 支持按多键分区，可以再叠一层 tenant_id。

### 一个潜在隐患：跨数据源事务

最后留一个已知未解决的隐患：`record(...)` 方法在同一个 `@Transactional` 里做了三件事——

```java
@Transactional
public TelemetryEvent record(RecordTelemetryCommand command) {
    // ...
    telemetryHotDataPort.append(event);                  // → 时序库（独立 DataSource）
    telemetrySnapshotRepository.save(snapshot);          // → 业务库
    applicationEventPublisher.publishEvent(...);         // → 进程内事件
    return event;
}
```

**时序库和业务库是两个独立 DataSource**，Spring 的 `@Transactional` 默认绑定主 DataSource，所以**时序库的写入实际上不在事务里**。如果业务库 commit 失败回滚，时序库已经写进去的数据会成为"孤儿数据"。

当前这个隐患可控的原因是：

- 时序库的 INSERT 是幂等的（`ON CONFLICT DO NOTHING`），重试不会重复写
- 业务库回滚的概率极低，绝大多数失败是上游 schema 校验，发生在 hotData 写入之前
- 即便孤儿数据真的产生，时序库的事件 id 在业务库找不到对应快照，下游也用不到

以后可以在链路里加入 outbox表，正好能把"先写业务库 + outbox，再异步搬到时序库"的路径走通。

---

> 项目地址：[https://github.com/Liyuwen85/iot-alarm-copilot](https://github.com/Liyuwen85/iot-alarm-copilot){:target="_blank"}