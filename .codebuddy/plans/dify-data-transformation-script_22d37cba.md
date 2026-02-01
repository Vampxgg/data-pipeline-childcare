---
name: dify-data-transformation-script
overview: Create a robust Python script for Dify to transform script and resource data into frontend-friendly formats (Outline, Subtitles, Data Sources), handling various input formats and ensuring data integrity.
todos:
  - id: create-script
    content: Create dify_transformation.py with main transformation logic
    status: completed
  - id: implement-helpers
    content: Implement safe_parse_json and format_time helpers
    status: completed
    dependencies:
      - create-script
  - id: implement-outline
    content: Implement extract_outline logic to process scenes and durations
    status: completed
    dependencies:
      - implement-helpers
  - id: implement-subtitles
    content: Implement extract_subtitles logic with timestamp accumulation
    status: completed
    dependencies:
      - implement-helpers
  - id: implement-sources
    content: Implement extract_data_sources logic to normalize references
    status: completed
    dependencies:
      - implement-helpers
  - id: create-tests
    content: Create test_transformation.py with mock data and ceshi.json content
    status: completed
    dependencies:
      - implement-sources
---

## Product Overview

Develop a robust Python script designed for Dify's code execution node. The script's primary function is to transform complex backend data (Script Data and Resource Data) into a streamlined, frontend-friendly JSON format used for rendering "Outline", "Subtitle", and "Data source" UI tabs.

## Core Features

- **Data Normalization**: robustly parse inputs that may be JSON strings or Python dictionaries.
- **Outline Generation**: Extract scene structures, titles, descriptions, and calculate formatted durations (MM:SS).
- **Subtitle Extraction**: Aggregate and format subtitles with timestamps (MM:SS) from nested scene data.
- **Data Source Formatting**: Standardize references from various keys (e.g., `web_data`, `references`, `network`) into a unified list with types (WEB/PDF), titles, and descriptions.
- **Error Handling**: Gracefully handle missing fields or unexpected data structures to ensure pipeline stability.

## Tech Stack

- **Language**: Python 3.x (Standard Library only, to ensure compatibility with Dify's sandbox environment).
- **Core Modules**: `json`, `typing`.

## Implementation Details

### Key Functions

- `safe_parse_json(input_data)`: Helper to convert string inputs to dictionaries.
- `format_time(seconds)`: Helper to convert integer seconds/milliseconds into "MM:SS" format.
- `transform_data(script_data, resource_data)`: Main entry point orchestrating the transformation.

### Data Flow

1. **Input**:

- `script_data`: Raw script JSON (Scenes, Subtitles).
- `resource_data`: Raw resource JSON (References, Web Data).

2. **Processing**:

- Parse and validate inputs.
- Iterate through `scenes` to build the **Outline** and accumulate **Subtitles**.
- Scan `resource_data` for known keys (`network`, `web_data`, `references`) to build **Data Sources**.

3. **Output**:

- Structured JSON with keys: `outline`, `subtitles`, `data_source`.