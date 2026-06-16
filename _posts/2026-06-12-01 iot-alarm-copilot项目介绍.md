---
title: 01 iot-alarm-copilot项目介绍
date: 2026-06-12 15:00:00 +0800
categories: [用DDD重构IoT后端系列]
tags: [IoT, 领域驱动设计]
---

**iot-alaram-copilot** 项目是笔者在学习IoT平台后端开发过程中，开源的样板示例工程；目标是切近生产环境，掌握IoT平台的业务设计和架构。项目实现从“设备模拟上报”、“Broker转发”、到“Kafka缓冲队列”、再到“后端接入层处理”、“热点时序数据分层存储”、“规则处理”、和“告警处置”等各环节功能，形成一条最小化的、相对完整的“上行”处理链路，加深学习与理解。

> 项目地址：[https://github.com/Liyuwen85/iot-alarm-copilot](https://github.com/Liyuwen85/iot-alarm-copilot){:target="_blank"}


本文主要介绍项目的整体结构，以及如何用“领域驱动设计”思想来指导整个IoT后端平台的建设。

## 目录
> 项目整体架构
>
> 领域驱动设计简介
> 
> DDD在本项目中的实践

## 项目整体架构
![架构图](/assets/img/posts/20260612/01-iot-alarm-copilot.png "整体架构图")
为了学习相对完整IoT平台知识体系，笔者增加了模拟设备和Broker中间件等南向基础设施，还完成了“上行”和“下行”双链路。整个架构图看起来挺复杂，实际上可划分为三部分：一. 模拟设备层；二. 中间件代理层；三. 后端处理链路。

另外，在技术实现上，也是尽量切近生产环境，所以做了多节点、高可用、数据冷热分层等功能，所以看起复杂了一点。

下面先简要介绍这三部分内容，以后**独立文章详细展开介绍**。

### 一、模拟设备层
本层主要的目标是：模拟多种协议设备上报遥测数据，以及接收平台命令、控制上报时间间隔等。模拟支持了MQTT、CoAP、Modbus、LwM2M协议，以及对模拟设备001和002的后台命令控制。
```
// 代码目录
mock-device-modbus   // 模拟设备002，实现modbus采集以及LwM2M客户端；
mock-device          // 两个职责：1. 模拟设备001；2. 模拟LwM2M网关，与002交互。
```
其中，`mock-device`也按照DDD的思想用纯java实现，仅依赖mqtt和lwm2m两个三方库，不依赖其它开发框架；很适合在脱离Spring依赖的环境下，学习DDD的开发精髓。

`mock-device-modbus`用Linux c来实现，本地跑在wsl上面，如果有c语言开发基础的同学，可以尝试编译运行。当然，在本项目联调时，只运行001设备，也完全可以满足后台开发需求。
![wsl调试图](/assets/img/posts/20260612/01-mock-device-modbus-debug.png "mock-device-modbus在wsl下调试")

### 二、中间件代理层
在生产环境中一般使用**EMQX**这种企业级的Broker中间件平台，来管理海量物联网设备连接、以及百万级消息处理与转发等。本项目开发环境中为了简洁，使用Mosquitto做为MQTT的消息代理中间件。

最开始，笔者在backend使用mqtt consumer直连broker来收集遥测事件；后面换成了用Kafka来接收broker上报的遥测数据，backend再消费kafka主题，实现解耦、削峰、死信等机制。

由于Mosquitto对kafka的支持比较麻烦，所以就构建了一个桥接工程`mqtt-kafka-bridge`，将broker中的MQTT消息转发到KafKa中，然后backend就可以直接消费kafka了。这个小工程也很简单，用postgreSQL的advisory lock应用层自定义锁机制，实现了多节点的热备和单活主节点选举策略，保证了高可用。
```java
......
// 获取锁
public boolean tryAcquire() {
    try {
        ensureConnected();
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
            statement.setLong(1, config.leaderLockKey());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getBoolean(1);
                }
                return false;
            }
        }
    } catch (SQLException exception) {
        closeQuietly();
        throw new IllegalStateException("Failed to acquire PostgreSQL advisory lock", exception);
    }
}
......
```

### 三、后端处理链路
`backend`工程按领域对象来建立多模块上下文；模块之间通过Telemetry、Alarm等事件进行数据处理和流转，保证了各自模块的独立性，以后演变为**微服务**架构时，模块边界可以保留，只需要把进程内事件总线替换成跨消息总线即可，不用涉及大量的代码改造。

| 模块  | 作用 |
| ----- | ----------- |
| iot-context-access  | 接入上下文，职责“设备怎么进入平台”，如：topic解析、接入身份验证、幂等键、协议到领域命令事件的转换等。|
| iot-context-telemetry  | 遥测上下文，负责原始遥测接入后的schema校验、标准化、聚合、最新状态等。|
| iot-context-device  | 设备上下文，负责设备注册、身份、影子、分组、生命周期、产品模型、物模型这类“设备主数据”。|
| iot-context-rule  | 规则上下文，负责规则定义、条件判断、命中结果。|
| iot-context-alarm  | 告警上下文，负责告警生成、告警状态流转、告警查询等。|
| iot-context-inspection  | 巡检上下文，负责基于告警生成巡检建议、工单预留、处理确认闭环。|
| iot-context-ai  | AI上下文，负责Prompt版本、摘要生成契约、结构化输出schema等。|
| iot-context-audit  | 审计上下文，负责关键事件留痕、操作审计、回访链路等。|
| iot-context-command  | 命令上下文，负责发送“下行”命令。|
| iot-integration-contract  | 共享集成，定义一些公共事件契约、通用事件等。|
| iot-persistence-support  | 持久化支持，主要完成MyBatis的自动装配等。 |
| iot-platform-boot  | 单体应用启动入口。 |


## 领域驱动设计简介

DDD（Domain-Driven Design）的核心是：**软件的复杂性来自业务领域本身，代码结构要反映业务模型，不能被技术框架实现或数据库表牵着走**。

落地时分两层：战略层（怎么切）、战术层（怎么写）。网上的DDD介绍有很多，大家可以自行搜索补齐知识。笔者只挑出与本项目上最相关的三个核心点展开：**领域划分**、**限界上下文划分**、**防腐层设计**，其它一些概念（实体、值对象、聚合、领域事件、仓储等）放到后续文章里再逐一拆解。

### 一、领域划分

DDD 把业务系统切成三类领域，目的是**有限的项目资源应该重点投入到哪里**。

| 类型 | 说明 | 投入策略 |
|---|---|---|
| **核心域（Core Domain）** | 业务竞争力所在，不可外包、不可妥协 | 最核心的资源投入 |
| **支撑子域（Supporting Subdomain）** | 服务于核心域，但本身不是差异化 | 标准实现即可 |
| **通用子域（Generic Subdomain）** | 任何系统都需要的通用能力 | 优先集成现成方案 |

具体到本项目，笔者是这么划分领域的：

| 模块 | 领域类型 | 理由 |
|---|---|---|
| iot-context-device | 核心域 | 设备主数据，平台根基 |
| iot-context-telemetry | 核心域 | 数据语义统一，平台核心价值 |
| iot-context-rule | 核心域 | 规则是平台与客户业务对齐的关键 |
| iot-context-alarm | 核心域 | 告警也是核心 |
| iot-context-inspection | 核心域 | 告警处置闭环 |
| iot-context-access | 支撑子域 | 协议适配重要，但非差异化 |
| iot-context-command | 支撑子域 | 下行通道，基础能力 |
| iot-context-ai | 支撑子域 | 增强体验但不决定平台生死 |
| iot-context-audit | 通用子域 | 几乎所有系统都需要 |
| iot-integration-contract | 通用子域 | 上下文契约共享 |
| iot-persistence-support | 通用子域 | 持久化技术支撑 |
| iot-platform-boot | 通用子域 | 启动入口 |

### 二、限界上下文划分

限界上下文（Bounded Context）是 DDD 战略层最重要、也是最难做对的事情。它的定义是**一个模型适用的边界**，如"商品"在销售上下文叫 SKU、在物流上下文叫包裹。但最主要的是要弄清楚：**边界到底按什么样的标准来划分**？

笔者在本项目里的标准是：**变化节奏一致的代码放在一起，节奏不同的必须分开；以及受众边界**这两个原则来划分的，如下表所示：

| 上下文 | 划分理由 |
|---|---|
| access | 跟着协议/厂家变,变化频率最高,失败模式独特(死信聚合)  |
| telemetry | 跟着业务语义变,有自己的 schema、派生指标、双层存储  |
| device | 主数据,变化最慢,有产品模型这种结构一致性最强的聚合  |
| rule | 配置驱动,可热更新,有自己的状态机(DRAFT/ACTIVE/INACTIVE)  |
| alarm | 状态机驱动 + 合规留痕,变化节奏与 rule 完全相反  |
| audit | 受众完全不同(业务/合规),保留策略不同(月-年),不影响业务正确性  |
| ai | AI能力会扩到巡检建议生成、规则推荐、异常检测、预测维护  |
| command | 独立“下行命令”是必须的  |

当然上面提到`iot-context-inspection`这个上下文独立出来有点多余，应该合并到alarm中，在状态中增加一个“已经工单”的状态，即可与alarm整合在一起。这是一点失误，当时建模块的时，看着顺眼就独立了。

### 三、防腐层设计

防腐层（Anti-Corruption Layer，简称 ACL）是上下文映射里最重要的一种设计方式，它的本质是**核心域不能直接依赖外部系统，必须有一个中间层来解耦**。

在IoT系统的"外部"是协议、设备厂家、固件版本、外部模型等，每一项都在以自己的结构定义，可能会遇到设备升级、协议更新等诸多不可控问题。ACL正好可以解决这个问题，通过适配层，屏蔽外部变化，让核心域专注于自身价值。笔者在本项目里把ACL设计了几个层次：

**第一层：协议适配 ACL（access 上下文承担）**

`iot-context-access` 整个上下文本质就是一个超大 ACL。它的全部职责是把任意协议、任意 broker 上报的字节流，翻译成核心域能消费的领域命令。本项目的 access 同时挂了 Kafka 和 MQTT（单测时用） 两个适配器，为下游屏蔽了协议细节。

**第二层：语义适配 ACL（telemetry 上下文承担）**

相同类型的多厂设备定义相差可能很大，如：A 厂的字段叫 `t`，B 厂叫 `temperature`，C 厂带单位后缀。telemetry 上下文负责把"原始字段"翻译成"平台标准化指标"。

**第三，失败也要被处理**

access 上下文有一个独立的死信聚合 `AccessDeadLetterLog`，用于处理消息失败的情况。它同时保留了 Kafka 的 partition/offset 和 MQTT 的 topic，这些都是"外部世界的事实"，但在 ACL 里被统一翻译成一个领域概念："接入失败事件"。这样运营同学就可以在后台查询"今天哪台设备最常失败"，而不是直接去查询Kafka的dead letter topic。

## DDD 在本项目中的实践

前面三个核心点（领域划分、限界上下文、防腐层）讲完了"为什么"，下面上代码，看看它们在 backend 工程里具体什么样的。

### 一、统一的“经典四层”模块目录

每个 `iot-context-*` 模块都按 DDD 经典分层切成：

```
iot-context-access/
├── application/        # 应用层：编排流程、事务控制
├── domain/             # 领域层：实体、值对象、领域服务、仓储接口
├── infrastructure/     # 基础设施层：数据库、消息中间件、外部 SDK
└── interfaces/         # 接口层：HTTP、Kafka 消费者、MQTT 监听
```

如果要找业务逻辑去 domain或application，找外部入口去 interfaces，找ORM去 infrastructure即可。本项目中的context都遵守同一套包结构。

### 二、上下文之间靠"端口契约"通信，不靠直接依赖

要划分限界上下文，前提是上下文之间不能互相import。本项目用一个独立的 `iot-integration-contract` 模块承担"上下文之间契约"，所有跨上下文的调用，调用方依赖 contract 里定义的**端口接口**，被调用方在自己上下文里实现这个接口。

比如：access 在接入遥测时需要查询设备的遥测模型，但它不能直接 import device 上下文。所以 contract 模块里定义了一个端口：

```java
// iot-integration-contract
public interface DeviceTelemetryModelQueryPort {
    Optional<DeviceTelemetryModel> findTelemetryModel(String deviceCode);
}
```

实现方在 device 上下文中：

```java
// iot-context-device
@Component
public class DeviceTelemetryModelQueryAdapter implements DeviceTelemetryModelQueryPort {
    private final DeviceRepository deviceRepository;
    private final ProductModelRepository productModelRepository;
    // ...
    @Override
    public Optional<DeviceTelemetryModel> findTelemetryModel(String deviceCode) {
        return deviceRepository.findByDeviceCode(new DeviceCode(deviceCode))
                .flatMap(this::toTelemetryModel);
    }
}
```

access 只看见接口，不看见 `DeviceRepository`、`ProductModel` 这些 device 内部概念。这样做有两个好处：

1. **替换实现零成本**，以后 device 拆成独立服务,只需要在 access 一侧新增一个实现 DeviceTelemetryModelQueryPort 的RPC 客户端,在 device 一侧把现有的 DeviceTelemetryModelQueryAdapter 包一层 HTTP/gRPC 接口对外暴露。access上下文业务中的调用代码一行不用改；
2. **依赖倒置**，device 反过来依赖 contract，access 也依赖 contract，两个上下文谁也不依赖谁。这就是 DDD 上下文映射里所说的“客户/供应商”关系。

### 三、access 是怎么做"协议无关入口"的

回到防腐层那节提到的两个适配器调用统一入口，在代码里是这样实现的：

`interfaces/kafka` 和 `interfaces/mqtt` 是两个独立的协议消费者，但它们都把协议层的字节交给 `TelemetryAccessApplicationService`：

```java
// iot-context-access/application
public class TelemetryAccessApplicationService {

    public void ingestMqttTelemetry(String topic, String payload) {
        ingestTelemetry(topic, payload);
    }

    public void ingestKafkaTelemetry(String topic, String payload) {
        ingestTelemetry(topic, payload);
    }

    public void ingestTelemetry(String topic, String payload) {
        TelemetryTopic telemetryTopic = new TelemetryTopic(topic);
        String topicDeviceId = telemetryTopic.deviceId();
        // 接入许可校验（依赖 device 上下文的端口）
        deviceTelemetryIngestionPort.ensureTelemetryIngestionAllowed(topicDeviceId);
        // 拿设备遥测模型（依赖 device 上下文的端口）
        DeviceTelemetryModel model = deviceTelemetryModelQueryPort
                .findTelemetryModel(topicDeviceId)
                .orElseThrow(...);
        // 按模型解析 payload
        TelemetryMessage message = telemetryMessageParser.parse(payload, model);
        TelemetryPayload normalized = telemetryIngressPolicy.normalize(telemetryTopic, message);
        // 调下游 telemetry 上下文
        telemetryIngestApplicationService.record(...);
    }
}
```

注意几个细节：

- `ingestMqttTelemetry` 和 `ingestKafkaTelemetry` 是两个不同协议入口，都直接转发到 `ingestTelemetry`。
- `TelemetryTopic` 是一个值对象，topic 解析失败抛领域异常——把"格式错误"翻译成"业务异常"，这正是 ACL的职责。
- 这个方法体内出现了三个端口`deviceTelemetryIngestionPort`、`deviceTelemetryModelQueryPort`、`telemetryIngestApplicationService`，分别来自 device、device、telemetry 上下文，access 自己什么主数据都没有，全靠端口调用。

### 四、上下文之间靠"领域事件"流转

端口适配器解决了"调用关系"，领域事件解决的是"通知关系"。本项目的上行主链是用一连串领域事件串起来的：

```
TelemetryRecorded  →  RuleTriggered  →  AlarmCreated  →  (并行) AlarmAiSummaryGenerated / InspectionTicketCreated / AuditLogged
```

事件契约统一定义在 `iot-integration-contract/contract/event` 目录下，每个事件都是一个不可变的 record，例如：

```java
public record TelemetryRecordedEvent(
        Long telemetryEventId,
        String deviceId,
        TelemetryMetrics metrics,
        Instant reportedAt) implements DomainEvent {

    public static final String EVENT_TYPE = "telemetry.recorded";

    @Override
    public Instant occurredAt() {
        return reportedAt;     // 业务时间，不是处理时间
    }
}
```

在当前的单体形态下，事件由 Spring 的 `ApplicationEventPublisher` 派发，下游上下文用 `@EventListener` 订阅。如 rule 上下文是这样消费遥测事件的：

```java
@Component
public class TelemetryRecordedRuleHandler {

    @EventListener
    public void onTelemetryRecorded(TelemetryRecordedEvent event) {
        var triggeredResults = telemetryRuleEvaluationApplicationService.evaluate(...);
        // ...
    }
}
```

这种以后**方便未来微服务化改造**，把 `ApplicationEventPublisher` 替换成 Kafka 生产者、`@EventListener` 替换成 Kafka 消费者，事件契约和业务代码一行不用改。

### 五、聚合根承担业务一致性

最后看上下文内部，**每个上下文里的核心业务一致性，由一个聚合根负责维护**。

举三个代表性聚合根：

| 聚合根 | 所属上下文 | 作用 |
|---|---|---|
| `Alarm` | alarm | 状态机合法性、防重键唯一性、时间字段一致性 |
| `ProductModel` | device | 能力声明、物模型、遥测模型、影子模型四件套自洽 |
| `Device` | device | 设备生命周期合法性、影子版本递增 |

以 `Alarm` 为例，它是一个 record，构造器里完成全部不变量校验：

```java
public record Alarm(
        Long id,
        AlarmDedupKey dedupKey,
        String ruleCode,
        // ... 其他字段
        AlarmStatus status,
        Instant triggeredAt,
        Instant acknowledgedAt,
        Instant closedAt) {

    public Alarm {
        Objects.requireNonNull(dedupKey, "dedupKey must not be null");
        // ... 其他空值校验
        AlarmStatusPolicy.validateLifecycle(
                status, triggeredAt, acknowledgedAt, closedAt);
    }

    public Alarm acknowledge(Instant acknowledgedAt) {
        AlarmStatusPolicy.ensureAcknowledgeAllowed(status);
        return new Alarm(/* 状态切换为 ACKED */);
    }

    public Alarm close(Instant closedAt) {
        AlarmStatusPolicy.ensureCloseAllowed(status);
        return new Alarm(/* 状态切换为 CLOSED */);
    }
}
```

几个关键设计：

- **聚合是不可变的**，每次状态变更都返回新对象，避免共享状态导致的隐蔽 bug；用Java record来实现；
- **状态机判断不在 if-else 块中**，交给 `AlarmStatusPolicy` 这个领域策略对象集中管理，功能清晰；
- **聚合自己负责防重**，`AlarmDedupKey` 由领域规则计算`ruleCode + deviceId + telemetryEventId`，下游数据库用它做唯一约束,就实现了**幂等性**。

---

> 项目地址：[https://github.com/Liyuwen85/iot-alarm-copilot](https://github.com/Liyuwen85/iot-alarm-copilot){:target="_blank"}