---
name: dify-industry-research-workflow
overview: 设计基于 Dify 的行业调研数据采集工作流，实现从问卷数据输入到知识库实时存储的自动化流程。包含数据结构定义、工作流架构设计及知识库写入方案。
todos:
  - id: define-schema
    content: 创建 survey_schema.json 定义调研数据采集标准结构
    status: completed
  - id: create-transform-script
    content: 开发 format_survey_for_rag.py 实现 JSON 到 Markdown 的转换逻辑
    status: completed
    dependencies:
      - define-schema
  - id: doc-workflow
    content: 编写 dify_workflow_design.md 详细说明工作流配置与 API 调用参数
    status: completed
    dependencies:
      - create-transform-script
---

## 需求概述

在 Dify 中实现“行业调研数据采集”的自动化工作流，支持从前端采集结构化数据，经过清洗转换后，实时写入 Dify 知识库 (RAG) 以供后续检索和问答使用。

## 核心功能

1.  **数据采集标准定义**: 基于行业调研需求（机构、个人、岗位、任务、能力、招聘等维度），定义标准化的 JSON Schema 数据结构。
2.  **数据清洗与转换**: 编写 Python 脚本（运行于 Dify Code Node），将采集的 JSON 数据转换为语义清晰、结构化的 Markdown 文本，提取关键元数据（如机构名称、岗位名称）。
3.  **自动化入库**: 设计 Dify Workflow，通过 HTTP Request 节点调用 Dify Dataset API，实现数据的实时向量化存储。
4.  **架构设计**: 确立“前端采集 -> Dify 转换 -> 知识库存储”的系统架构。

## 技术架构

采用 **ETL (Extract, Transform, Load)** 模式构建数据管道：

1.  **数据源 (Extract)**: 前端表单或问卷系统收集数据，输出符合 Schema 定义的 JSON 对象。
2.  **数据处理 (Transform)**: Dify Workflow 中的 **Code Node (Python)**。

    -   **输入**: JSON 字符串。
    -   **逻辑**: 解析 JSON，处理空值，按业务逻辑拼接为 Markdown 格式。
    -   **输出**: 格式化后的 Markdown 文本、文档名称（如“调研-某机构-某岗位”）。

3.  **数据存储 (Load)**: Dify Workflow 中的 **HTTP Request Node**。

    -   **目标**: Dify Knowledge Base API (`/datasets/{dataset_id}/document/create_by_text`).
    -   **格式**: 纯文本/Markdown。

## 存储结构设计

### 1. 原始数据 (JSON Schema)

用于前后端交互的数据契约，包含六大核心模块：

-   `institution`: 机构基本信息（名称、性质、规模...）
-   `respondent`: 受访者信息（姓名、职位、从业年限...）
-   `job_info`: 岗位信息（名称、所属部门...）
-   `core_tasks`: 核心工作任务（列表）
-   `capabilities`: 能力要求（知识、技能、素养...）
-   `recruitment`: 招聘与培养（学历、专业、晋升路径...）

### 2. 知识库文档 (Markdown)

用于 RAG 检索的文档格式，强调语义分割和标题层级，利于 LLM 理解上下文：

```markdown
# 行业调研报告：[机构名称] - [岗位名称]

## 1. 机构概况
- **名称**: ...
- **类型**: ...

## 2. 核心任务
1. [任务名称]: [任务描述]
...

## 3. 能力画像
- **专业知识**: ...
- **核心技能**: ...
```

## 实现方案细节

-   **JSON 解析**: 复用 `dify_transformation.py` 中的 `safe_parse_json` 逻辑增强鲁棒性。
-   **API 调用**: 使用 Dify 自身的 API Key 进行回环调用写入知识库。