---
name: refactor-childcare-pipeline-dual-mode
overview: "Refactor data pipeline scripts to support dual input modes: the existing legacy structure (for internal use) and a new split structure (for external childcare institutions), automatically adapting processing and output based on the input format."
todos:
  - id: refactor-link-parser
    content: Refactor `_intelligent_input_parser` in `多数据源获取链接.py` to detect Internal/External modes
    status: completed
  - id: refactor-link-execution
    content: Update `main_async` in `多数据源获取链接.py` to execute searches for `general_web_query` and `institution_source_query` and format output accordingly
    status: completed
    dependencies:
      - refactor-link-parser
  - id: refactor-data-parser
    content: Update `_parse_input_data` in `多数据源获取数据.py` to extract and tag URLs from `general_web_data` and `institution_source_data`
    status: completed
    dependencies:
      - refactor-link-execution
  - id: refactor-data-execution
    content: Update `main_async` in `多数据源获取数据.py` to group scraped results by origin and construct the corresponding output structure
    status: completed
    dependencies:
      - refactor-data-parser
---

## Product Overview

Refactor the existing data pipeline scripts (`多数据源获取链接.py` and `多数据源获取数据.py`) to intelligently support two distinct operational modes based on input structure: **Legacy/Internal Mode** and **New/External Mode**.

## Core Features

- **Intelligent Input Detection**: Automatically detect the operating mode (Internal vs. External) by analyzing the input JSON structure in both scripts.
- **Dual Mode Execution in Link Acquisition**:
- **Internal Mode**: Process `comprehensive_query`, `career_query`, and `tianyan_check_enterprise`. Output `comprehensive_data`, `career_data`, and `tianyan_check_data`.
- **External Mode**: Process `general_web_query`, `institution_source_query`, and `career_query`. Output `general_web_data`, `institution_source_data`, and `career_data`.
- **Dual Mode Execution in Data Scraping**:
- Parse the output from the link acquisition step, identifying the source of each URL (e.g., from `general_web_data` vs `comprehensive_data`).
- Scrape content and organize the final output into the corresponding structure (`general_web_data` + `institution_source_data` vs `comprehensive_data`).

## Tech Stack

- **Language**: Python 3
- **Libraries**: `asyncio`, `httpx`, `json`, `re` (Existing stack)

## Implementation Details

### `多数据源获取链接.py` (Link Acquisition)

- **Input Parser Update**: Modify `_intelligent_input_parser` to check for `general_web_query` and `institution_source_query`. If present, flag as External Mode. If `comprehensive_query` is present, flag as Internal Mode.
- **Execution Logic**:
- In `main_async`, create search tasks dynamically based on non-empty input lists.
- Reuse `searcher.web_search` for the new `general_web_query` and `institution_source_query` lists.
- Ensure `career_query` is processed in both modes.
- Ensure `tianyan_check_enterprise` is processed only if present (Internal Mode).
- **Output Construction**: Build the `datas` dictionary dynamically to match the detected mode's required schema.

### `多数据源获取数据.py` (Data Scraping)

- **Input Parser Update**: Modify `_parse_input_data` to iterate through `general_web_data` and `institution_source_data` (in addition to `comprehensive_data`).
- **Data Tagging**: When extracting URLs, tag each item with its `origin_key` (e.g., "general_web_data", "institution_source_data") to track its source group.
- **Output Reconstruction**: In `main_async`, after scraping, group the results back into their respective categories based on the tags and construct the final JSON output to match the input mode.