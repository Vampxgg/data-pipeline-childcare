---
name: format_survey_for_dify
overview: 将 format_survey_for_rag.py 改造为适配 Dify 代码执行节点的格式，增强 JSON 解析健壮性和字段提取的容错性。
todos:
  - id: update-script
    content: Refactor format_survey_for_rag.py with safe_parse_json and robust extraction logic
    status: completed
---

## User Requirements

- **Enhance `format_survey_for_rag.py`**: Modify the script to be compatible with Dify's code execution environment.
- **Robust Input Parsing**: Implement `safe_parse_json` to handle various input formats (JSON string, dictionary, Markdown code blocks, dirty JSON) robustly.
- **Data Extraction & Formatting**: Ensure all fields defined in `survey_schema.json` are correctly extracted and formatted into Markdown, including conditional fields like `manager_specific_info` and `transition_needs`.
- **Error Handling**: Add comprehensive try-catch blocks to prevent script failures and return meaningful error messages.

## Functional Overview

The script will accept raw survey data (string or object), parse it into a structured dictionary, and generate a readable Markdown report suitable for RAG (Retrieval-Augmented Generation) indexing. It will handle missing fields gracefully and format complex nested structures (like recruitment needs) into clear lists.

## Tech Stack

- **Language**: Python 3 (Standard Library only, to ensure compatibility with Dify's restricted sandbox).
- **Key Modules**: `json`, `re`, `datetime`, `traceback`.

## Implementation Details

1.  **Input Normalization**:

    - Port `safe_parse_json` from `dify_transformation.py` to handle string/dict inputs and strip Markdown formatting.
    - Ensure the `main` function can accept a single argument (the input variable from Dify).

2.  **Robust Data Access**:

    - Use a `safe_get` utility or defensive `.get()` chaining to access nested fields without raising `AttributeError`.
    - Default to "N/A" or empty strings for missing optional data.

3.  **Markdown Generation**:

    - Structure the Markdown output with clear headers (#, ##, ###).
    - Implement specific formatting logic for lists of objects (e.g., shortage positions, certificate requirements).
    - Add conditional logic to only show sections relevant to the specific user role (e.g., only show "Manager View" for principals).

4.  **Error Handling**:

    - Wrap the entire execution in a `try...except` block.
    - Return a dictionary with `error` key on failure, rather than crashing, to allow the Dify workflow to handle the failure branch.