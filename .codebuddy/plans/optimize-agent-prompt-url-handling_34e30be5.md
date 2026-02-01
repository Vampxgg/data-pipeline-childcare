---
name: optimize-agent-prompt-url-handling
overview: 优化“场景数据.md”中的 Prompt，增强对视频链接（URL）的处理逻辑，强制生成包含 `site:`、视频ID、标题组合等丰富多样的检索关键词，解决检索指令单一的问题。
todos:
  - id: update-prompt-path1
    content: 修改场景数据 Prompt，重写 Path 1 逻辑，增加 URL 解构步骤、强制检索模板及 Few-Shot 示例
    status: completed
---

## User Requirements

用户指出 `e:/Vampxgg/数据管道pipeline/托育/场景数据.md` 中的智能体 Prompt 在处理视频链接时表现“偷懒”，仅使用原始链接进行搜索，导致效果不佳。

## Core Features

用户要求优化 Prompt 的 **Path 1 (URL处理模式)**，具体修改如下：

1.  **增加 URL 解构步骤**：在 Thought 阶段明确要求提取 Domain、Video ID 和 Title。
2.  **强制多样化检索指令**：在 Call 阶段强制要求 `web_queries` 必须包含以下组合：

    -   原始 URL
    -   `site:domain video_id` (源站精准定位)
    -   Video ID + "summary/transcript" (内容扩展)
    -   标题/关键词 (模糊搜索)

3.  **增加 Few-Shot Examples**：提供具体示例以指导模型生成正确的检索词。

## Goal

通过 Prompt Engineering 提升智能体对 URL 输入的解析深度和检索广度，确保获取更丰富的视频元数据和内容。

## Implementation Approach

本次修改主要涉及 **Prompt Engineering** 的优化，采用以下策略：

1.  **Chain of Thought (CoT) 增强**：

    -   在 `Path 1` 的 **Thought** 环节增加显式的 "URL Deconstruction" (URL 解构) 步骤，强迫模型先分析 URL 结构（提取 ID、域名等）再行动，避免直接跳到工具调用。

2.  **Constraint Enforcement (约束强化)**：

    -   将原本模糊的 `Call` 指令修改为**结构化模板**。明确规定 `web_queries` 的 4 个槽位必须填入特定类型的 Search Query，消除模型“偷懒”的空间。

3.  **Few-Shot Learning (少样本学习)**：

    -   在 `Path 1` 中直接嵌入 Input -> Generated Queries 的具体示例（Example），利用 LLM 的上下文学习能力，确保其理解 `site:` 语法和 ID 提取的逻辑。

## Modification Scope

-   **Target File**: `e:/Vampxgg/数据管道pipeline/托育/场景数据.md`
-   **Section**: `## Path 1: 链接深度解析 (当输入是 URL)`