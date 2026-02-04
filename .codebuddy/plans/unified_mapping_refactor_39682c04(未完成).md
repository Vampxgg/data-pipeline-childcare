---
name: unified_mapping_refactor
overview: 创建一个新的 `mappings.py` 文件来统一管理区域、学历和专业映射，并实现智能的区域层级匹配逻辑（省份-城市包含关系），随后在 `retrieve.py` 中集成该模块以替换旧的硬编码逻辑。
todos:
  - id: create-mappings
    content: Create mappings.py with CHINA_REGIONS, EDUCATION_MAP, and UnifiedMetaManager class
    status: pending
  - id: refactor-retrieve
    content: Refactor retrieve.py to use UnifiedMetaManager for region and education logic
    status: pending
    dependencies:
      - create-mappings
---

## User Requirements

- **Unify Mappings**: Consolidate Region, Education, and Major mappings into a new `mappings.py` module for centralized maintenance.
- **Region Logic**: 
    - Support diverse formats (e.g., "四川", "四川省", "四川-成都", "成都市").
    - Implement hierarchical matching: Province query matches all subordinate cities; City query matches only that city.
    - **Coverage**: Must include all Chinese prefecture-level cities (comprehensive list).
- **Education Logic**: Migrate existing `EDUCATION_MAP` and normalization logic to the new module.
- **Major Logic**: Initialize an empty `MAJOR_MAP` with placeholders/comments for future use.
- **Refactoring**: Update `retrieve.py` to use the new `UnifiedMetaManager` for rule checking and normalization, replacing hardcoded maps and simple string matching.

## Core Features

- **Centralized Metadata Manager**: `UnifiedMetaManager` class handling all mapping and matching logic.
- **Smart Region Matching**: Logic to parse and match locations hierarchically (Province > City).
- **Education Normalization**: Ported logic for standardizing education levels.
- **Seamless Integration**: Refactored `TuoyuProcessor` in `retrieve.py` to delegate logic to the new manager.

## Tech Stack

- **Language**: Python 3.x
- **Components**: 
    - `mappings.py`: New module for data and logic.
    - `retrieve.py`: Existing module to be refactored.

## Implementation Approach

1.  **`mappings.py`**:

    - **Data Structures**:
        - `CHINA_REGIONS`: A large dictionary `{Province: [City1, City2, ...]}` containing all prefecture-level cities.
        - `EDUCATION_MAP`: Moved from `retrieve.py`.
        - `MAJOR_MAP`: Empty dictionary.
    - **`UnifiedMetaManager` Class**:
        - `__init__`: Builds reverse indexes (City -> Province) for fast lookup.
        - `normalize_location(name)`: Strips suffixes like "省", "市", "自治区" for consistent matching.
        - `parse_location(text)`: Extracts Province and City from strings like "四川-成都".
        - `check_region_match(rule_scope, doc_scope)`: Implements the hierarchical logic (Province matches sub-cities; City matches exact).
        - `normalize_education(text)`: Ported normalization logic.

2.  **`retrieve.py` Refactoring**:

    - Import `UnifiedMetaManager`.
    - Instantiate `UnifiedMetaManager` in `TuoyuProcessor`.
    - Replace `EDUCATION_MAP` and `normalize_education` method with calls to the manager.
    - Update `check_rules` to use `manager.check_region_match` for scope verification.

## Implementation Notes

- **Region Data**: Will generate a comprehensive list of Chinese provinces and cities to ensure coverage.
- **Normalization**: "吉林" (Province) vs "吉林" (City) ambiguity will be handled by prioritizing Province matching or context inference, but given the hierarchical rule, matching Province "吉林" covers "吉林市", which is safe.
- **Performance**: Build lookup dictionaries (`city_to_province`) once during initialization to ensure O(1) matching during high-volume processing.
- **Backward Compatibility**: Ensure `check_rules` signature remains compatible with existing callers if any, though it seems internal to `TuoyuProcessor`.

## Directory Structure

```
d:/Codeing/pythonprojects/tuoyu/data-pipeline-childcare/
├── mappings.py      # [NEW] Unified metadata and matching logic. Contains CHINA_REGIONS, EDUCATION_MAP, UnifiedMetaManager.
└── retrieve.py      # [MODIFY] Refactor TuoyuProcessor to use mappings.py. Remove hardcoded maps and old matching logic.
```