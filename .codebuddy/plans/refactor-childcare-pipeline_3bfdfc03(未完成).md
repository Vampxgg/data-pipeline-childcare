---
name: refactor-childcare-pipeline
overview: Refactor existing data pipeline scripts to support a specialized structure for childcare institutions, distinguishing between general web data and institution source data.
todos:
  - id: refactor-link-retrieval
    content: Refactor `多数据源获取链接.py` to separate General Web and Institution Source queries and outputs
    status: pending
  - id: refactor-content-scraping
    content: Refactor `多数据源获取数据.py` to process and structure separated data streams
    status: pending
    dependencies:
      - refactor-link-retrieval
---

## Product Overview

Refactor the existing childcare data pipeline to support a more granular data structure, specifically separating general web data from institution-specific source data, while maintaining existing capabilities for position data and Tianyan checks.

## Core Features

- **Structured Input Parsing**: Update input parsing logic to distinguish between "General Web" queries and "Institution Source" queries.
- **Dual-Stream Search Execution**: Execute search workflows independently for general web queries and institution source queries to maintain data separation.
- **Segregated Data Scraping**: Process scraping tasks separately for the two new data streams to ensure the final output structure reflects the separation.
- **Unified Output Formatting**: Generate a structured JSON output that clearly categorizes data into `general_web_content`, `institution_source_content`, `career_postings`, and `enterprise_infos`.

## Tech Stack

- **Language**: Python 3.x
- **Libraries**: `httpx`, `asyncio`, `trafilatura`, `beautifulsoup4` (existing stack)

## Implementation Details

### `多数据源获取链接.py` (Link Retrieval)

- **Input Parser**: Update `_intelligent_input_parser` to accept `general_web_query` and `institution_source_query` instead of the single `comprehensive_query`.
- **Search Logic**: In `main_async`, invoke `searcher.web_search` twice: once for general queries and once for institution queries.
- **Output Structure**:

```
{
"datas": {
"general_web_data": [...],
"institution_source_data": [...],
"career_data": {...},
"tianyan_check_data": [...]
}
}
```

### `多数据源获取数据.py` (Content Scraping)

- **Input Parser**: Update `_parse_input_data` to extract URLs separately from `general_web_data` and `institution_source_data`.
- **Orchestrator**: Update processing logic (or `main_async` workflow) to handle the two distinct lists of URLs concurrently but separately to track their origins.
- **Output Structure**:

```
{
"scraped_datas": {
"general_web_content": { "all_source_list": [...], "all_video_list": [...] },
"institution_source_content": { "all_source_list": [...], "all_video_list": [...] },
"career_postings": {...},
"enterprise_infos": {...}
}
}
```