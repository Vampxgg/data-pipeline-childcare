---
name: format_survey_for_rag_redesign
overview: 重新设计 format_survey_for_rag.py，使其能够处理行业调研数据采集-20260127-简版.docx 中出现的所有情况，并与 survey_schema.json 和 formConfig.ts 保持一致。
todos:
  - id: rewrite-format-script
    content: Redesign format_survey_for_rag.py with config parsing and schema-driven Markdown generation
    status: completed
---

## User Requirements

- **Redesign `format_survey_for_rag.py`**: Create a robust Python script to convert survey JSON data into Markdown for RAG (Retrieval-Augmented Generation).
- **Comprehensive Coverage**: Handle all sections and fields defined in `survey_schema.json`, including nested objects and lists.
- **Data Mapping**: Integrate logic to parse `formConfig.ts` and map raw codes (e.g., "private", "director") to human-readable labels (e.g., "民办", "园长/负责人") using the frontend configuration.
- **Role Adaptability**: Dynamically handle fields specific to different roles (Director, Teacher, Caregiver, etc.) as they appear in the data.
- **Robustness**: Improve error handling and JSON parsing resilience.

## Product Overview

A data transformation utility that takes raw survey JSON (conforming to `survey_schema.json`) and produces a structured, human-readable Markdown report. This report serves as high-quality input for a RAG system, ensuring that the LLM can understand the survey responses in their proper context with correct terminology.

## Core Features

- **Dynamic Configuration Parsing**: Extracts label mappings (Options, Matrices) directly from `formConfig.ts`.
- **Schema-Driven Formatting**: Generates Markdown sections corresponding to:
- Institution Overview
- Personal & Employment Information
- Position Details (Core Tasks, Capabilities, Qualities)
- Manager Specifics (Medical-Education, Recruitment, Training)
- **Value Transformation**: Automatically converts code values to labels using the extracted mappings.
- **Conditional Rendering**: Displays sections only when data is present, keeping the report clean.

## Tech Stack

- **Language**: Python 3
- **Libraries**: `json`, `re`, `os`, `datetime`, `traceback` (Standard Library only, to ensure portability in Dify)

## Implementation Approach

1.  **Configuration Extraction**: Implement a regex-based parser (adapted from `transform_survey_data.py`) to read `data/formConfig.ts` and build a dictionary of mappings (`code` -> `label`).
2.  **Schema-Config Bridge**: Define a `SCHEMA_TO_CONFIG_MAP` dictionary to link `snake_case` keys from the JSON schema to `camelCase` keys in the frontend config.
3.  **Data Transformation Phase**: Before formatting, traverse the input JSON. For each field, check if a mapping exists (via the bridge) and translate values (handling strings, lists, and matrix objects).
4.  **Markdown Generation Phase**:

    - Use a modular approach with helper functions for each major section (`format_institution`, `format_personal`, etc.).
    - Use data-driven list formatting to handle variable-length arrays (e.g., `shortage_positions`).
    - Explicitly handle known sections from `survey_schema.json` to ensure logical flow.

5.  **Execution Context**: The script will be designed to run both as a standalone script (reading local files) and as a module (accepting input/config).

## Directory Structure

```
project-root/
├── format_survey_for_rag.py  # [MODIFY] Complete rewrite. Includes config parsing, data transformation, and Markdown generation logic.
└── data/
├── formConfig.ts         # [READ] Source of mapping definitions.
├── survey_schema.json    # [REFERENCE] Definition of data structure.
└── test_survey_data.json # [REFERENCE] Test input.
``````