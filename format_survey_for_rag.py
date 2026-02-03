import json
import re
import os
import datetime
import traceback

# ==========================================
# 1. Configuration & Mappings
# ==========================================

# Map snake_case fields in JSON Schema to camelCase keys in formConfig.ts
SCHEMA_TO_CONFIG_KEY = {
    # Institution Info
    "institution_info.name": "orgName",
    "institution_info.city": "location",
    "institution_info.subject_type": "orgNature",
    "institution_info.specific_form": "orgType",
    "institution_info.is_puhui": "isPovertyFree",
    "institution_info.service_modes": "serviceMode",
    "institution_info.total_capacity": "totalSlots",
    "institution_info.current_enrollment": "totalChildren",
    "institution_info.staff_count": "totalStaff",
    
    # Personal Info
    "personal_info.gender": "gender",
    "personal_info.education": "education",
    "personal_info.major": "educationMajor",
    
    # Employment Info
    "employment_info.current_position": "currentPosition",
    "employment_info.job_change_interval": "interval",
    "employment_info.job_change_reasons": "reason",
    "employment_info.salary_range": "salaryRange",
    "employment_info.is_kindergarten_transition": "isFromTeacherToTeacher",
    "employment_info.transition_needs": "reasonFromTeacherToTeacher", # Note: Schema says needs, config says reason. Keeping mapping for safety.
    
    # Position Details
    "position_details.core_tasks": "coreTasks",
    "position_details.capability_requirements": ["trainingNeeds", "careSkills"], # Might be one of these
    "position_details.quality_requirements": "competency_matrix",
    
    # Manager Specific Info (Partial mappings based on available config)
    "manager_specific_info.future_talent_needs": "futureTalentNeeds",
    "manager_specific_info.suggestions": "suggestions"
}

# ==========================================
# 2. Helper Functions: Config Parsing
# ==========================================

def parse_ts_config(file_path):
    """
    Parses formConfig.ts to extract label mappings.
    Returns: { 'field_key': { 'type': 'options/matrix', 'map': {...} } }
    """
    if not os.path.exists(file_path):
        print(f"Warning: Config file not found at {file_path}")
        return {}

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    mappings = {}
    
    # 1. Find all keys
    key_pattern = re.compile(r"key:\s*['\"]([^'\"]+)['\"]")
    keys = []
    for match in key_pattern.finditer(content):
        keys.append((match.group(1), match.start()))
    
    # 2. Extract options for each key
    for i, (key_name, start_pos) in enumerate(keys):
        end_pos = keys[i+1][1] if i + 1 < len(keys) else len(content)
        block_content = content[start_pos:end_pos]
        
        # A. Options (Select/Radio/Checkbox)
        options_match = re.search(r"options:\s*\[(.*?)\]", block_content, re.DOTALL)
        if options_match:
            options_str = options_match.group(1)
            opt_map = {}
            # Match label:'...', value:'...'
            items = re.finditer(r"label:\s*['\"]([^'\"]+)['\"].*?value:\s*(['\"]?)([^'\"}\s,]+)\2", options_str, re.DOTALL)
            for item in items:
                label = item.group(1)
                val = item.group(3)
                opt_map[val] = label
            
            if opt_map:
                mappings[key_name] = {'type': 'options', 'map': opt_map}
                continue

        # B. Matrix
        rows_match = re.search(r"rows:\s*\[(.*?)\]", block_content, re.DOTALL)
        cols_match = re.search(r"columns:\s*\[(.*?)\]", block_content, re.DOTALL)
        
        if rows_match and cols_match:
            row_map = {}
            col_map = {}
            
            for item in re.finditer(r"label:\s*['\"]([^'\"]+)['\"].*?value:\s*(['\"]?)([^'\"}\s,]+)\2", rows_match.group(1), re.DOTALL):
                row_map[item.group(3)] = item.group(1)
            
            for item in re.finditer(r"label:\s*['\"]([^'\"]+)['\"].*?value:\s*(['\"]?)([^'\"}\s,]+)\2", cols_match.group(1), re.DOTALL):
                col_map[item.group(3)] = item.group(1)
                
            mappings[key_name] = {'type': 'matrix', 'rows': row_map, 'cols': col_map}
    print(mappings)
    return mappings

def get_label(value, config_key, mappings):
    """
    Translates a value (code) to a label using the mappings.
    """
    if value is None or value == "":
        return "N/A"
        
    # Handle boolean -> Yes/No if mapped, otherwise default
    if isinstance(value, bool):
        # Check if there's a mapping for 'yes'/'no' or 'true'/'false'
        # Usually frontend sends 'yes'/'no' strings for radios, but JSON might have bools
        pass 

    if config_key not in mappings:
        # Special handling for booleans if no mapping
        if isinstance(value, bool):
            return "是" if value else "否"
        return str(value)

    config = mappings[config_key]
    
    if config['type'] == 'options':
        mapping = config['map']
        if isinstance(value, list):
            return ", ".join([mapping.get(str(v), str(v)) for v in value])
        else:
            # Try exact match, then string match
            return mapping.get(value, mapping.get(str(value), str(value)))
            
    elif config['type'] == 'matrix':
        if isinstance(value, dict):
            # Format matrix as a list of "Row: Col"
            items = []
            for r_k, c_k in value.items():
                r_label = config['rows'].get(str(r_k), str(r_k))
                c_label = config['cols'].get(str(c_k), str(c_k))
                items.append(f"{r_label}: {c_label}")
            return "; ".join(items)
            
    return str(value)

# ==========================================
# 3. Data Processing & Markdown Generation
# ==========================================

def safe_get(data, path, default=None):
    curr = data
    for key in path.split('.'):
        if isinstance(curr, dict) and key in curr:
            curr = curr[key]
        else:
            return default
    return curr

def format_section_list(title, items):
    if not items:
        return ""
    md = f"### {title}\n"
    for item in items:
        md += f"- {item}\n"
    md += "\n"
    return md

def format_survey_data(survey_data, mappings):
    """
    Main function to format the survey dict into Markdown.
    """
    lines = []
    
    # --- Header ---
    inst_name = safe_get(survey_data, "institution_info.name", "未知机构")
    position = safe_get(survey_data, "employment_info.current_position", "未知岗位")
    # Translate position if possible
    position = get_label(position, "currentPosition", mappings)
    
    city = safe_get(survey_data, "institution_info.city", "未知城市")
    date_str = datetime.date.today().isoformat()
    
    lines.append(f"# 行业调研报告：{inst_name} - {position}")
    lines.append(f"> 采集日期: {date_str} | 城市: {city}")
    lines.append("")

    # --- 1. 机构概况 ---
    lines.append("## 1. 机构概况")
    info = survey_data.get("institution_info", {})
    
    fields_1 = [
        ("名称", "name", "orgName"),
        ("性质", "subject_type", "orgNature"),
        ("形态", "specific_form", "orgType"),
        ("普惠", "is_puhui", "isPovertyFree"),
        ("服务模式", "service_modes", "serviceMode"),
        ("规模", None, None) # Composite field
    ]
    
    for label, key, config_key in fields_1:
        if key:
            val = info.get(key)
            display_val = get_label(val, config_key, mappings)
            lines.append(f"- **{label}**: {display_val}")
        elif label == "规模":
            cap = info.get("total_capacity", 0)
            enr = info.get("current_enrollment", 0)
            stf = info.get("staff_count", 0)
            lines.append(f"- **规模**: 托位 {cap} / 在园 {enr} / 员工 {stf}")
    lines.append("")

    # --- 2. 受访者画像 ---
    lines.append("## 2. 受访者画像")
    personal = survey_data.get("personal_info", {})
    employ = survey_data.get("employment_info", {})
    
    gender = get_label(personal.get("gender"), "gender", mappings)
    edu = get_label(personal.get("education"), "education", mappings)
    major = personal.get("major", "N/A")
    lines.append(f"- **基本信息**: {gender} | {edu} ({major})")
    
    curr_pos = get_label(employ.get("current_position"), "currentPosition", mappings)
    lines.append(f"- **当前岗位**: {curr_pos}")
    if employ.get("current_position_other"):
        lines.append(f"  - 备注: {employ.get('current_position_other')}")
        
    salary = get_label(employ.get("salary_range"), "salaryRange", mappings)
    lines.append(f"- **薪资范围**: {salary}")
    
    interval = get_label(employ.get("job_change_interval"), "interval", mappings)
    lines.append(f"- **换岗频率**: {interval}")
    
    reasons = employ.get("job_change_reasons")
    if reasons:
        reasons_str = get_label(reasons, "reason", mappings)
        lines.append(f"- **换岗原因**: {reasons_str}")
        
    is_trans = employ.get("is_kindergarten_transition")
    if is_trans: # Only show if true or present
        trans_str = get_label(is_trans, "isFromTeacherToTeacher", mappings)
        lines.append(f"- **幼儿园转型**: {trans_str}")
        needs = employ.get("transition_needs")
        if needs:
             lines.append(f"- **转型提升需求**: {needs}")
    lines.append("")

    # --- 3. 岗位详情 ---
    pos_details = survey_data.get("position_details", {})
    if pos_details:
        lines.append("## 3. 岗位详情")
        
        # Core Tasks
        tasks = pos_details.get("core_tasks")
        if tasks:
            # Try to translate tasks if they are keys
            # Assuming 'coreTasks' config exists
            task_labels = []
            if isinstance(tasks, list):
                # We need to map each item individually because get_label for list joins them
                # But here we want a markdown list
                config = mappings.get("coreTasks", {})
                mapping = config.get('map', {})
                for t in tasks:
                    task_labels.append(mapping.get(str(t), str(t)))
            else:
                task_labels.append(str(tasks))
            
            lines.append(format_section_list("核心工作任务", task_labels))
            
        # Capability Requirements
        caps = pos_details.get("capability_requirements")
        if caps:
            # Mapping might be 'trainingNeeds' or 'careSkills' depending on role
            # We try both or just show raw if not found
            # A simple heuristic: check if values match keys in trainingNeeds
            cap_labels = []
            if isinstance(caps, list):
                # Try finding a mapping
                found_map = {}
                for key in ["trainingNeeds", "careSkills"]:
                    if key in mappings:
                        m = mappings[key]['map']
                        # Check if any cap is in this map
                        if any(str(c) in m for c in caps):
                            found_map = m
                            break
                
                for c in caps:
                    cap_labels.append(found_map.get(str(c), str(c)))
            else:
                cap_labels.append(str(caps))
            
            lines.append(format_section_list("能力/培训需求", cap_labels))
            
        # Quality Requirements (Matrix)
        quals = pos_details.get("quality_requirements")
        if quals:
            lines.append("### 素质素养要求")
            if isinstance(quals, dict):
                # Use get_label logic for matrix but format as list
                config = mappings.get("competency_matrix", {})
                if config.get('type') == 'matrix':
                    for r_k, c_k in quals.items():
                        r_label = config['rows'].get(str(r_k), str(r_k))
                        c_label = config['cols'].get(str(c_k), str(c_k))
                        lines.append(f"- {r_label}: **{c_label}**")
                else:
                    for k, v in quals.items():
                        lines.append(f"- {k}: {v}")
            elif isinstance(quals, list):
                for q in quals:
                    lines.append(f"- {q}")
            lines.append("")

    # --- 4. 管理视角 (园长/负责人) ---
    manager = survey_data.get("manager_specific_info", {})
    if manager:
        lines.append("## 4. 管理视角")
        
        # 4.1 医育结合
        med = manager.get("medical_education_combination", {})
        if med:
            lines.append("### 医育结合")
            forms = med.get("forms", [])
            if forms:
                lines.append(f"- **开展形式**: {', '.join(forms)}")
            if med.get("partner_institutions"):
                lines.append(f"- **合作机构**: {med.get('partner_institutions')}")
            if med.get("cooperation_details"):
                lines.append(f"- **合作详情**: {med.get('cooperation_details')}")
            lines.append("")

        # 4.2 招聘与培养
        rec = manager.get("recruitment_training", {})
        if rec:
            lines.append("### 招聘与培养")
            
            # Shortage Positions
            shortage = rec.get("shortage_positions", [])
            if shortage:
                lines.append("- **紧缺岗位**:")
                for item in shortage:
                    p = item.get("position", "未知")
                    c = item.get("count", 0)
                    lines.append(f"  - {p}: {c}人")
            
            # Education Reqs
            edu_reqs = rec.get("education_requirements", [])
            if edu_reqs:
                lines.append("- **学历要求**:")
                for item in edu_reqs:
                    p = item.get("position", "未知")
                    e = item.get("education", "N/A")
                    lines.append(f"  - {p}: {e}")
            
            # Certificate Reqs
            cert_reqs = rec.get("certificate_requirements", [])
            if cert_reqs:
                lines.append("- **证书要求**:")
                for item in cert_reqs:
                    p = item.get("position", "未知")
                    cs = item.get("certificates", [])
                    cs_str = ", ".join(cs) if isinstance(cs, list) else str(cs)
                    lines.append(f"  - {p}: {cs_str}")
            
            # Other Lists
            for field_key, field_label in [
                ("recruitment_channels", "招聘渠道"),
                ("priority_factors", "优先因素"),
                ("training_needs", "培训需求"),
                ("effective_training_modes", "有效培养模式")
            ]:
                val = rec.get(field_key)
                if val:
                    val_str = ", ".join(val) if isinstance(val, list) else str(val)
                    lines.append(f"- **{field_label}**: {val_str}")
            
            if rec.get("graduate_issues"):
                lines.append(f"- **毕业生问题**: {rec.get('graduate_issues')}")
                
        # 4.3 Future & Suggestions (from Config Step 5)
        future = manager.get("future_talent_needs") # Schema key might differ, checking logic
        # Schema doesn't explicitly list 'future_talent_needs' in the snippet I saw, 
        # but 'step5Fields' in config has it. 
        # If it's in the data under manager_specific_info (or root?), we handle it.
        # Assuming it might be in manager_specific_info based on context.
        if future:
             # Try mapping
             future_str = get_label(future, "futureTalentNeeds", mappings)
             lines.append(f"- **未来人才需求**: {future_str}")
             
        sugg = manager.get("suggestions")
        if sugg:
            lines.append(f"- **建议**: {sugg}")

    return "\n".join(lines)


# ==========================================
# 4. Metadata Extraction
# ==========================================

def extract_metadata(survey_data, mappings):
    """
    Extracts structured metadata for RAG filtering.
    Returns a flat dictionary suitable for vector database metadata.
    """
    info = survey_data.get("institution_info", {})
    personal = survey_data.get("personal_info", {})
    employ = survey_data.get("employment_info", {})
    
    # Helper to get label or raw value
    def _get(val, key):
        return get_label(val, key, mappings)

    metadata = {
        # Institution Filters
        "org_name": info.get("name"),
        "city": info.get("city"),
        "org_nature": _get(info.get("subject_type"), "orgNature"),
        "org_type": _get(info.get("specific_form"), "orgType"),
        "is_puhui": _get(info.get("is_puhui"), "isPovertyFree"),
        
        # Personal Filters
        "gender": _get(personal.get("gender"), "gender"),
        "education": _get(personal.get("education"), "education"),
        "major": personal.get("major"),
        
        # Job Filters
        "position": _get(employ.get("current_position"), "currentPosition"),
        "salary_range": _get(employ.get("salary_range"), "salaryRange"),
        "job_change_interval": _get(employ.get("job_change_interval"), "interval"),
        
        # Timestamp
        "processed_at": datetime.date.today().isoformat()
    }
    return metadata

# ==========================================
# 5. Main Execution
# ==========================================

def main(survey_data_input, config_path=None):
    """
    Entry point.
    args:
        survey_data_input: dict or json string
        config_path: path to formConfig.ts
    """
    # 1. Parse Input
    if isinstance(survey_data_input, str):
        try:
            survey_data = json.loads(survey_data_input)
        except:
            # Try simple cleanup
            try:
                clean = survey_data_input.strip()
                if clean.startswith("```json"):
                    clean = clean[7:-3].strip()
                elif clean.startswith("```"):
                    clean = clean[3:-3].strip()
                survey_data = json.loads(clean)
            except Exception as e:
                return {"error": f"Invalid JSON input: {e}"}
    else:
        survey_data = survey_data_input

    # 2. Parse Config
    mappings = {}
    if config_path and os.path.exists(config_path):
        try:
            mappings = parse_ts_config(config_path)
        except Exception as e:
            print(f"Error parsing config: {e}")
            # Continue without mappings
    
    # 3. Format
    try:
        markdown = format_survey_data(survey_data, mappings)
        metadata = extract_metadata(survey_data, mappings)
        
        # Generate filename
        inst = safe_get(survey_data, "institution_info.name", "Survey")
        pos = safe_get(survey_data, "employment_info.current_position", "User")
        date = datetime.date.today().strftime("%Y%m%d")
        doc_name = f"调研报告_{inst}_{pos}_{date}"
        
        return {
            "markdown_content": markdown,
            "metadata": metadata,
            "document_name": doc_name
        }
    except Exception as e:
        return {
            "error": str(e),
            "traceback": traceback.format_exc()
        }

if __name__ == "__main__":
    # Local Test
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    
    config_file = os.path.join(data_dir, "formConfig.ts")
    test_file = os.path.join(data_dir, "test_survey_data.json")
    
    if os.path.exists(test_file):
        with open(test_file, 'r', encoding='utf-8') as f:
            raw_data = f.read()
            
        result = main(raw_data, config_file)
        
        if "markdown_content" in result:
            print("--- Generated Metadata ---")
            print(json.dumps(result.get("metadata", {}), ensure_ascii=False, indent=2))
            print("\n--- Generated Markdown ---")
            print(result["markdown_content"])
            
            # Save to file for verification
            out_file = os.path.join(data_dir, "test_output_rag.md")
            with open(out_file, 'w', encoding='utf-8') as f:
                f.write(result["markdown_content"])
            print(f"\nSaved to {out_file}")
        else:
            print("Error:", result)
    else:
        print("Test file not found.")
