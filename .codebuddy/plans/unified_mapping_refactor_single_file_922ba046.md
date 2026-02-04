---
name: unified_mapping_refactor_single_file
overview: 在 `retrieve.py` 内部封装 `UnifiedMetaManager` 类来统一管理区域、学历和专业映射，并实现智能的区域层级匹配逻辑，不创建新文件。
todos:
  - id: add-meta-manager
    content: Implement UnifiedMetaManager class with CHINA_REGIONS data and parsing logic in retrieve.py
    status: completed
  - id: refactor-processor
    content: Refactor TuoyuProcessor to use UnifiedMetaManager for region and education checks
    status: completed
    dependencies:
      - add-meta-manager
---

## User Requirements

- **Unify Mappings**: Centralize Region, Education, and Major mappings into a single manager within `retrieve.py`.
- **Region Mapping Logic**:
- **Hierarchical Matching**:
    - **Province Level**: Querying a province (e.g., "Sichuan") matches the province itself and all its cities (e.g., "Chengdu", "Nanchong").
    - **City Level**: Querying a city (e.g., "Chengdu") only matches that specific city.
- **Robust Parsing**: Handle various input formats (e.g., "四川", "四川省", "四川-成都", "成都市", "四川省成都市").
- **Data Coverage**: Include a comprehensive dictionary of all Chinese provinces and prefecture-level cities.
- **Education Mapping**: Functionally identical to the existing `EDUCATION_MAP` but moved to the new manager.
- **Major Mapping**: Reserve an empty structure for future use with comments.

## Constraints

- **File Structure**: Do NOT create a new file. Implement all changes within `retrieve.py`.

## Implementation Approach

We will encapsulate all metadata logic into a new class `UnifiedMetaManager` within `retrieve.py`. This class will hold the static mapping data and provide methods for parsing and matching.

### 1. Data Structures

- **`CHINA_REGIONS`**: A dictionary `{ "ProvinceName": ["City1", "City2", ...] }`.
- *Note*: Will include all provinces and prefecture-level cities.
- *Optimization*: Create a reverse lookup map `CITY_TO_PROVINCE` during initialization for O(1) city-to-province resolution.
- **`EDUCATION_MAP`**: Moved from `TuoyuProcessor`.
- **`MAJOR_MAP`**: Empty dictionary `{}`.

### 2. `UnifiedMetaManager` Class

- **`__init__`**: Initializes the reverse city lookup map.
- **`parse_location(text)`**:
- Cleans input (removes "省", "市", "自治区", etc. for matching).
- Identifies Province and City from the text.
- Returns `(province, city)` tuple.
- *Logic*: Scans for city names first (more specific), then province names. Handles combined strings like "四川成都".
- **`check_region_match(rule_scope, doc_scope)`**:
- Parses both `rule_scope` (user query/filter) and `doc_scope` (document metadata).
- **Logic**:

    1. If `rule_city` is present: Match only if `doc_city == rule_city`.
    2. If `rule_province` is present (and no `rule_city`): Match if `doc_province == rule_province`.
    3. If `rule_scope` is empty: Match all.

### 3. Integration

- Refactor `TuoyuProcessor`:
- Instantiate `UnifiedMetaManager`.
- Replace `EDUCATION_MAP` usage with `manager.normalize_education()`.
- Replace manual string matching in `check_rules` with `manager.check_region_match()`.

## Algorithm: Region Matching

```python
def check_region_match(self, rule_text, doc_text):
    r_prov, r_city = self.parse_location(rule_text)
    d_prov, d_city = self.parse_location(doc_text)

    # 1. City-level strict match
    if r_city:
        return d_city == r_city

    # 2. Province-level inclusion match
    if r_prov:
        return d_prov == r_prov

    return True # No rule constraint
```