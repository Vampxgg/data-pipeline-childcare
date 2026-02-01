---
name: agent-response-optimization-plan
overview: Refactor the agent's response logic and tool invocation strategy in `video generate(线上).yml` to separate internal reasoning from user-facing notifications using an XML-based protocol, and make tool usage more intelligent and flexible.
todos:
  - id: construct-prompt
    content: 构建包含 XML 协议、拟人化规则和智能决策逻辑的全新 System Prompt
    status: completed
  - id: update-yaml
    content: 将新 Prompt 注入 video generate(线上).yml 的 instruction 字段，并确保 YAML 缩进正确
    status: completed
    dependencies:
      - construct-prompt
---

## Product Overview

重构 `video generate(线上).yml` 中的 Agent 核心指令（System Prompt），将其从一个机械的工具执行者升级为具备独立思考能力的“首席创意总监”。

## Core Features

- **XML 协议分离**：严格实施 `<thinking>`（内部思考、决策、工具参数构建）与 `<notification>`（用户可见回复）的分离，确保技术细节不泄露给用户。
- **拟人化交互**：摒弃模板化回复（如“正在调用工具...”），采用更具共情力、创造力和场景感知能力的自然语言与用户沟通。
- **智能决策工作流**：
    - 从线性的“Step 1 -> Step 2”转变为“感知 -> 思考 -> 决策 -> 行动”的动态逻辑。
    - 增加**能力协商机制**：当用户需求超出工具能力时，主动构思并提出替代方案，而不是直接拒绝。
- **鲁棒性增强**：优化错误处理逻辑，当工具调用失败时，通过 Notification 引导用户或自动尝试修复，而非暴露底层报错。

## Prompt Engineering Architecture

### 1. XML Protocol (双层思维协议)

采用 "Three-Layer Burger" 结构，强制 Agent 在输出任何回复前先进行深度思考。

```xml
<thinking>
  [Internal Monologue]
  1. Intent Analysis: 用户真正想要什么？(e.g., "换个激昂音乐" -> 意图是"增强氛围")
  2. Tool Strategy: 现有工具能否直接满足？
     - Yes -> 构造 Call 参数
     - No -> 构思替代方案 (e.g., 改色调、加快语速)
  3. Language Check: 确认 user_intent 语种，锁定 Notification 语言。
</thinking>

<notification>
  [User Facing]
  基于思考结果的自然语言回复。
  - 拒绝机械感："收到，正在处理" (Bad) -> "这个想法很棒！把色调调暖确实能增加秋日的氛围..." (Good)
  - 包含具体主题词 (Topic Echo)。
</notification>

// Tool Calls (System Level)
{ ... json tool call ... }
```

### 2. State Machine Logic (状态机逻辑)

指令将包含明确的逻辑分支，指导 Agent 处理不同场景：

- **Creation Path**: 意图识别 -> (可选搜索) -> 剧本生成 -> 视频渲染。
- **Modification Path**: 意图分析 -> 映射到 `code_editor` 参数 -> 执行修改。
- **Negotiation Path**: 识别不支持需求 -> 思考替代方案 -> 通过 Notification 提案 (不调用工具)。

### 3. Role Definition

- **Role**: Chief Creative Director (首席创意总监)
- **Tone**: Professional, Empathetic, Creative, Proactive.