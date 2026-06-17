---
title: 08 用 Spring AI 把 LLM 调用做成领域聚合
date: 2026-06-16 10:00:00 +0800
categories: [用DDD重构IoT后端系列]
tags: [IoT, 领域驱动设计, Spring AI, LLM]
---

LLM 进入企业系统这两年，最常见的写法是把 OpenAI / 通义千问当一个 HTTP API 调用——拼 prompt、call、parse、写库。这种写法在 demo 里跑得通，在生产环境里会遇到一连串问题。本文从 ai 上下文里的 `AiSummaryTask` 聚合出发，讲清楚**怎么把 LLM 调用本身做成一个有领域语义的异步任务**。

> 项目地址：[https://github.com/Liyuwen85/iot-alarm-copilot](https://github.com/Liyuwen85/iot-alarm-copilot){:target="_blank"}

## 目录
> AI 摘要建模成异步任务聚合
>
> claim 模式 + 拆事务：LLM 调用不能放进 DB 事务
>
> Prompt 是领域规则，不是字符串
>
> LLM 输出也需要防腐层

## AI 摘要建模成异步任务聚合

本项目里 AI 摘要不是"调用 API 然后保存"，是一个完整的领域聚合 `AiSummaryTask`，有自己的状态机：

```
PENDING ──claim──▶ PROCESSING ──succeed──▶ SUCCEEDED
                       │
                       └──fail────────────▶ FAILED
```

四个状态对应四个领域含义：

- `PENDING`——告警来了，需要生成摘要，但还没开始算
- `PROCESSING`——已经被 worker 认领，正在调 LLM
- `SUCCEEDED`——LLM 返回了合格的结构化结果
- `FAILED`——LLM 调用失败或返回不可用，记录原因

`AiSummaryTask` 是 record 形式的聚合根，字段有点多：

```java
public record AiSummaryTask(
        Long id,
        Long alarmId,
        String alarmDedupKey,        // 防重键，跟 alarm 上下文一致
        String ruleCode,
        String deviceId,
        String severity,
        AiSummaryStatus status,
        Integer attemptCount,        // 已重试次数
        // —— 成功路径产物 ——
        String summary,
        String possibleCause,
        String inspectionSuggestion,
        String riskLevel,
        BigDecimal confidence,
        String modelName,            // 用哪个模型算的
        String promptVersion,        // 用哪个版本的 prompt
        String requestPayload,       // 完整 prompt 留档
        String responsePayload,      // 完整原始返回留档
        // —— 失败路径产物 ——
        String errorCode,
        String errorMessage,
        // —— 时间戳 ——
        Instant alarmTriggeredAt,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant finishedAt) { ... }
```

注意几个关键的字段：

- **`attemptCount`**，重试次数；
- **`modelName` + `promptVersion`**，本次调用用了哪个模型、哪个版本的 prompt；
- **`requestPayload` + `responsePayload`**，完整 prompt 和原始返回都留档。LLM 输出有概率性，没有原始留档就没办法复盘；
- **`startedAt` 和 `finishedAt` 分开**，前者是 claim 时间，后者是 succeed/fail 时间，差值就是 LLM 调用耗时。

### 构造器里的"成功必须完整"

`AiSummaryTask` 的构造器有一段特别的校验，**`SUCCEEDED` 状态的任务，所有产物字段必须非空**：

```java
public AiSummaryTask {
    // ... 基础校验
    if (status == AiSummaryStatus.SUCCEEDED) {
        ensureSucceeded(summary, possibleCause, inspectionSuggestion, riskLevel,
                confidence, modelName, promptVersion, requestPayload, responsePayload, finishedAt);
    }
    if (status == AiSummaryStatus.FAILED) {
        ensureFailed(modelName, promptVersion, requestPayload, errorCode, errorMessage, finishedAt);
    }
}
```

一旦任务到达 SUCCEEDED 状态，它一定有完整的摘要内容、模型名、prompt 版本、原始留档。下游订阅 `AlarmAiSummaryGeneratedEvent` 时不必再担心"成功了但 summary 是 null"这种残缺数据。

任务有四个状态，但 `SUCCEEDED` 和 `FAILED` 不是简单的"完成/失败"标记，它们各自要求一组完整字段相应变化。

## claim 模式 + 拆事务：LLM 调用不能放进 DB 事务

`AiSummaryApplicationService.generateIfPending` 这个方法把整条 LLM 调用拆成**三段独立事务**，避免了"同步阻塞"和"重试边界"：

```java
public void generateIfPending(GenerateAiSummaryCommand command) {
    // 第一段事务：claim
    Optional<AiSummaryTask> claimedTask = transactionTemplate.execute(status -> {
        AiSummaryTask currentTask = aiSummaryTaskRepository.load(command.taskId());
        AiSummaryTask processingTask = currentTask.claim(command.requestedAt());
        AiSummaryTaskStatusUpdateResult updateResult =
                aiSummaryTaskRepository.updateStatusIfCurrentStatusMatches(
                        processingTask, currentTask.status());
        return updateResult.changed() ? Optional.of(updateResult.task()) : Optional.empty();
    });
    if (claimedTask == null || claimedTask.isEmpty()) {
        return;   // 没抢到，别人在做了
    }

    // 第二段（无事务）：调 LLM
    AiSummaryTask processingTask = claimedTask.get();
    String prompt = AiPromptTemplatePolicy.buildAlarmSummaryPrompt(
            processingTask, aiProperties.getPromptVersion());
    try {
        LlmAlarmSummaryResult llmResult = llmGateway.generateAlarmSummary(...);
        AiSummaryTask succeededTask = processingTask.succeed(...);

        // 第三段事务：写回成功
        transactionTemplate.execute(status -> aiSummaryTaskRepository
                .updateStatusIfCurrentStatusMatches(succeededTask, AiSummaryStatus.PROCESSING));

        publishGenerated(succeededTask);
    } catch (Exception exception) {
        AiSummaryTask failedTask = processingTask.fail(...);
        // 第三段事务：写回失败
        transactionTemplate.execute(status -> aiSummaryTaskRepository
                .updateStatusIfCurrentStatusMatches(failedTask, AiSummaryStatus.PROCESSING));
        publishFailed(failedTask);
    }
}
```

可查看 [AiSummaryApplicationService.java 源码](https://github.com/Liyuwen85/iot-alarm-copilot/blob/main/backend/iot-context-ai/src/main/java/com/example/iotalarmcopilot/ai/application/AiSummaryApplicationService.java){:target="_blank"}

注意这里不是 `@Transactional`，而是用 `TransactionTemplate` **手动控制三个独立事务**。这件事非常关键，下面分别讲。

### 第一段事务：claim 是抢任务，不做修改

`claim` 这一步用前面讲过的"前置状态当版本号"做并发控制，`updateStatusIfCurrentStatusMatches(processingTask, AiSummaryStatus.PENDING)`。

如果两个 worker 同时尝试 claim 同一个 PENDING 任务，只有一个能成功，另一个的 update 影响 0 行，应用层拿到 `changed = false`，直接 return。

这是分布式 worker 抢任务的标准做法，把"我要做这个任务"建模成一次原子的状态切换，让数据库帮你拦住竞争。它让"哪个 worker 在做哪个任务"成为数据库层的事实，不需要 Redis 锁、不需要 Zookeeper、不需要消息队列的可见性超时。

### 第二段：LLM 调用不能放在事务里

为什么 LLM 调用必须脱离任何DB事务？三个理由：

**1. 事务时间窗口不能由外部服务决定**

数据库连接池是有限的。一次 LLM 调用可能 3 秒、可能 30 秒、可能超时。如果这段时间事务还开着，连接被占住，并发一上来就直接挂。

**2. 事务的 ACID 保护对外部调用没用**

事务能保证回滚 DB 操作，但回滚不了已经发出去的 HTTP 请求。

**3. 异常处理逻辑会被事务搅乱**

`@Transactional` 默认对 RuntimeException 回滚，但 AI 调用失败不是要回滚，失败本身是要记录的领域事实（任务进入 FAILED 状态）。如果整个方法在一个事务里，"记录失败"这一步会被事务回滚机制干掉。

### 第三段事务：写回结果，仍然用前置状态当版本号

不管成功还是失败，最终的状态写回都用 `updateStatusIfCurrentStatusMatches(_, AiSummaryStatus.PROCESSING)` 做并发保护。

这一步的作用是，如果这条任务从 PROCESSING 被异常切到了别的状态（比如运维手动重置），写回时会拿到 `changed = false`，应用层就知道"这个结果不该写回了"。LLM 调用是慢的，慢调用期间任何外部干预都可能让前提失效，状态机要拦住这种失效。

### 这套设计的整体效果

整个 `generateIfPending` 流程对外呈现的性质：

- **幂等**，重复调用同一个任务，第二次 claim 失败、直接返回；
- **可重试**，失败任务进入 FAILED 状态，可以由外部触发器重新创建一条 PENDING 任务再算（attemptCount 累加）；
- **不阻塞数据库**，LLM 调用期间没有事务、没有锁；
- **失败可观察**，失败也写回数据库、也发事件。

这一套机制可以用在**任何"调用慢、失败概率高、需要可观察"的外部依赖都该这么处理**。LLM 是这种依赖的极端形态：调用最慢、失败原因最丰富、成本最敏感。

## Prompt 是领域规则，不是字符串

本项目把 prompt 模板放在 `AiPromptTemplatePolicy` 这个领域策略类里，把它当作领域规则使用（后面可抽取为外部模板）：

```java
public final class AiPromptTemplatePolicy {

    private AiPromptTemplatePolicy() {}

    public static String buildAlarmSummaryPrompt(AiSummaryTask task, String promptVersion) {
        return """
                你是一名面向运维工程师的物联网告警助手。
                请分析告警信息，并仅返回有效的 JSON 格式数据，不要包含 Markdown 代码块标记。

                要求的 JSON 结构如下：
                {
                  "summary": "字符串",
                  "possibleCause": "字符串",
                  "inspectionSuggestion": "字符串",
                  "riskLevel": "LOW|MEDIUM|HIGH|CRITICAL",
                  "confidence": 0.0
                }

                约束条件：
                - summary: 用一句话简明扼要地总结告警
                - possibleCause: 用一句话简明扼要地说明可能原因
                - inspectionSuggestion: 用一句话列出2-3条可操作的排查建议
                - riskLevel: 必须是 LOW、MEDIUM、HIGH 或 CRITICAL 中的一个
                - confidence: 0 到 1 之间的小数，表示置信度
                - 不要编造不可用的遥测数据细节

                提示词版本: %s

                告警输入:
                {
                  "alarmId": %d,
                  "alarmDedupKey": "%s",
                  ...
                }
                """.formatted(promptVersion, task.alarmId(), ...);
    }
}
```

为啥是领域规则？LLM 的 prompt 决定了三件事：

- **输出的 JSON 结构**，`riskLevel` 只能是哪几个枚举、`confidence` 的取值范围；
- **业务的解释口径**，"summary 用一句话、inspectionSuggestion 列 2-3 条"这些约束是产品诉求，不是技术细节；
- **可解释性的边界**，"不要编造不可用的遥测数据细节" 这种 guard 是合规要求。

这三件事发生变化，业务行为就发生了变化。它的位置应该和 `AlarmStatusPolicy`、`AlarmDedupKeyPolicy` 平级，都是领域策略，集中管控、版本化、可追溯。

### Prompt 模板版本是 `AiProperties` 的一部分

`AiProperties.promptVersion` 是配置驱动的，改一次配置文件就发布新版本，不需要改代码。结合"每条任务记 promptVersion"，整个 AI 摘要的演进就有了完整的版本管理：

- 改 prompt 模板 → 改 `AiPromptTemplatePolicy` 里的字符串 + 改 `AiProperties.promptVersion`；
- 老任务还是用旧 promptVersion 留档；
- 新任务用新 promptVersion 留档；
- 可对比、可回滚、可解释。

因为 LLM 输出有概率性，没有版本化的可解释性等于没有可解释性，所以这套对LLM是必须的。

## LLM 输出也需要防腐层

本项目把 LLM 调用本身做成了一个**端口 + ACL** 的标准结构，让"调用过程可观测"。

端口定义在 infrastructure 层，但只暴露领域语义：

```java
public interface LlmGateway {
    LlmAlarmSummaryResult generateAlarmSummary(LlmAlarmSummaryRequest request);
}
```

它有两个实现：

```java
// 真实模型实现
public class SpringAiLlmGateway implements LlmGateway { ... }

// 开关关闭时的 no-op
public class DisabledLlmGateway implements LlmGateway { ... }
```

应用服务只看见 `LlmGateway`，不管背后是 OpenAI、通义千问、Claude、还是本地 Ollama，换模型只需要改实现层一处配置，业务代码一行不动。

### 输出解析也是 ACL

`SpringAiLlmGateway` 里有一个细节：

```java
@Override
public LlmAlarmSummaryResult generateAlarmSummary(LlmAlarmSummaryRequest request) {
    String rawResponse = chatClient.prompt(new Prompt(request.prompt()))
            .call()
            .content();
    AiStructuredSummary structuredSummary = parseStructuredSummary(rawResponse);
    return new LlmAlarmSummaryResult(
            structuredSummary,
            aiProperties.getModel(),
            request.promptVersion(),
            rawResponse);
}

private AiStructuredSummary parseStructuredSummary(String rawResponse) {
    try {
        JsonNode rootNode = objectMapper.readTree(stripJsonFence(rawResponse));
        return new AiStructuredSummary(
                readText(rootNode, "summary"),
                readText(rootNode, "possibleCause"),
                readText(rootNode, "inspectionSuggestion"),
                readText(rootNode, "riskLevel"),
                rootNode.path("confidence").decimalValue());
    } catch (IOException exception) {
        throw new IllegalStateException("Failed to parse AI structured summary JSON", exception);
    }
}

private String stripJsonFence(String rawResponse) {
    String trimmed = rawResponse == null ? "" : rawResponse.trim();
    if (trimmed.startsWith("```json")) {
        trimmed = trimmed.substring(7).trim();
    } else if (trimmed.startsWith("```")) {
        trimmed = trimmed.substring(3).trim();
    }
    if (trimmed.endsWith("```")) {
        trimmed = trimmed.substring(0, trimmed.length() - 3).trim();
    }
    return trimmed;
}
```

注意 `stripJsonFence` 这个看起来奇怪，那是由于LLM 经常会无视 prompt 里的"不要 markdown 代码块标记"指示，照样在 JSON 外面包一层 ```json。

### `DisabledLlmGateway` 说明

`DisabledLlmGateway` 是个空实现，开关关闭时生效，可以让本地开发和 CI 环境不依赖外部模型也能跑。

### 失败也是事件

`AiSummaryApplicationService` 在 LLM 调用失败时不是吞掉异常，而是发布 `AlarmAiSummaryFailedEvent`：

```java
private void publishFailed(AiSummaryTask task) {
    applicationEventPublisher.publishEvent(new AlarmAiSummaryFailedEvent(
            task.id(),
            task.alarmId(),
            task.alarmDedupKey(),
            task.ruleCode(),
            task.deviceId(),
            task.severity(),
            task.errorCode(),
            task.errorMessage(),
            task.finishedAt()));
}
```

下游（audit）订阅这个事件留痕，运营可以用领域查询接口去问"昨天 AI 摘要失败了多少条、是什么原因"。**失败不是异常，是领域事实**，这条原则在 之前access 死信处的设计里已经讲过，AI 上下文这里再次出现。

---

> 项目地址：[https://github.com/Liyuwen85/iot-alarm-copilot](https://github.com/Liyuwen85/iot-alarm-copilot){:target="_blank"}