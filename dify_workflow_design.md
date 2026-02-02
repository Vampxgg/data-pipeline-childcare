# Dify 行业调研数据采集工作流设计

## 1. 工作流概述
本工作流旨在实现行业调研数据的自动化采集与入库。
- **输入**: 符合 `survey_schema.json` 标准的 JSON 数据。
- **处理**: 使用 Python 代码节点将 JSON 转换为 Markdown 格式。
- **输出**: 调用 Dify API 将 Markdown 写入知识库，并返回处理结果。

## 2. 节点配置详情

### 节点 1: 开始 (Start)
- **输入变量**:
  - `survey_json` (String): 包含完整调研数据的 JSON 字符串。
  - `api_key` (String): Dify Dataset API Key (用于鉴权写入知识库)。
  - `dataset_id` (String): 目标知识库 ID。

### 节点 2: 代码执行 (Code - Python)
- **目的**: 数据清洗与格式转换。
- **输入变量**: `arg1` -> `survey_json`
- **代码逻辑**: 
  (直接复制 `format_survey_for_rag.py` 中的 `main` 函数及辅助函数)
  
  ```python
  import json
  import datetime
  
  # ... (粘贴 format_survey_for_rag.py 的全部内容) ...
  
  def main(arg1):
      return main_process(arg1) # 假设脚本里的入口函数改名为 main_process 以避免冲突，或者直接使用脚本里的 main
  ```
- **输出变量**:
  - `markdown_content` (String)
  - `document_name` (String)
  - `error` (String, optional)

### 节点 3: 逻辑判断 (If-Else)
- **条件**: `Code.error` is empty
- **True**: 进入 HTTP 请求节点。
- **False**: 直接结束，返回错误信息。

### 节点 4: HTTP 请求 (HTTP Request)
- **目的**: 调用 Dify API 创建文档。
- **API Endpoint**: `POST https://api.dify.ai/v1/datasets/{dataset_id}/document/create_by_text`
  - *注意*: 如果是私有部署，请替换为实际域名，如 `http://host.docker.internal/v1/...`
- **Headers**:
  - `Authorization`: `Bearer {{start.api_key}}`
  - `Content-Type`: `application/json`
- **Body (JSON)**:
  ```json
  {
    "name": "{{Code.document_name}}",
    "text": "{{Code.markdown_content}}",
    "indexing_technique": "high_quality",
    "process_rule": {
      "mode": "automatic"
    }
  }
  ```

### 节点 5: 结束 (End)
- **输出变量**:
  - `status`: "success" / "failed"
  - `doc_name`: `{{Code.document_name}}`
  - `message`: `{{HTTP.body}}` 或 错误信息

## 3. 部署与测试
1. **创建知识库**: 在 Dify 中创建一个新的知识库 (Dataset)。
2. **获取 API Key**: 在知识库设置中生成 API Key。
3. **导入代码**: 将 `format_survey_for_rag.py` 的内容填入 Code 节点。
4. **测试运行**: 使用示例 JSON (参考 Schema) 进行测试，验证知识库中是否生成了新的文档。

## 4. 附录：示例输入 JSON
```json
{
  "institution_info": {
    "name": "未来之星托育中心",
    "city": "上海",
    "subject_type": "民办",
    "specific_form": "独立托育机构",
    "is_puhui": true,
    "service_modes": ["全日托"],
    "total_capacity": 100,
    "current_enrollment": 85,
    "staff_count": 15
  },
  "personal_info": {
    "gender": "女",
    "education": "普通本科",
    "major": "学前教育"
  },
  "employment_info": {
    "current_position": "主班教师",
    "job_change_interval": "3-5年",
    "salary_range": "6000-8000",
    "is_kindergarten_transition": false
  }
}
```
