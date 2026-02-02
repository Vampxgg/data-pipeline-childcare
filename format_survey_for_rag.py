import json
import datetime
import re
import traceback

def safe_parse_json(input_data):
    """
    尝试将输入解析为字典或列表，处理字符串、Markdown代码块、包含额外文本的情况。
    """
    if isinstance(input_data, (dict, list)):
        return input_data
    
    if isinstance(input_data, str):
        input_data = input_data.strip()
        # 1. 尝试直接解析
        try:
            return json.loads(input_data)
        except json.JSONDecodeError:
            pass
            
        # 2. 尝试提取 Markdown 代码块 (```json ... ```)
        match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', input_data, re.IGNORECASE)
        if match:
            try:
                return json.loads(match.group(1))
            except:
                pass
        
        # 3. 尝试寻找最外层的 JSON 对象或数组
        try:
            start_brace = input_data.find('{')
            start_bracket = input_data.find('[')
            
            start = -1
            if start_brace != -1 and start_bracket != -1:
                start = min(start_brace, start_bracket)
            elif start_brace != -1:
                start = start_brace
            elif start_bracket != -1:
                start = start_bracket
                
            if start != -1:
                # 简单启发式：找最后一个对应的闭合符号
                end_brace = input_data.rfind('}')
                end_bracket = input_data.rfind(']')
                end = max(end_brace, end_bracket)
                
                if end > start:
                    potential_json = input_data[start:end+1]
                    return json.loads(potential_json)
        except:
            pass
            
    # 如果都失败了，返回空字典
    return {}

def safe_get(data, keys, default=""):
    """Safely get nested dictionary values."""
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
            if current is None:
                return default
        else:
            return default
    return current if current is not None else default

def format_list(items, title):
    """Format a list of strings into a Markdown list."""
    if not items or not isinstance(items, list):
        return ""
    
    md = f"### {title}\n"
    has_content = False
    
    for item in items:
        if isinstance(item, str) and item.strip():
            md += f"- {item}\n"
            has_content = True
        elif isinstance(item, dict):
            # Handle complex objects like shortage_positions
            parts = []
            for k, v in item.items():
                if v:
                    parts.append(f"{k}: {v}")
            if parts:
                md += f"- {', '.join(parts)}\n"
                has_content = True
                
    if has_content:
        md += "\n"
        return md
    return ""

def main(survey_data_input):
    """
    Main function to transform survey JSON data into Markdown for RAG.
    Designed for Dify code execution.
    
    Args:
        survey_data_input (str/dict): The input JSON data
        
    Returns:
        dict: Contains 'markdown_content' and 'document_name'
    """
    try:
        # 1. Robust Parsing
        survey_data = safe_parse_json(survey_data_input)
        
        if not isinstance(survey_data, dict) or not survey_data:
            # Fallback: if parsing failed but input is not empty, maybe return error
            return {
                "markdown_content": "Error: Invalid JSON input or empty data.",
                "document_name": "Error_Report"
            }

        # Extract basic info for title
        inst_name = safe_get(survey_data, ["institution_info", "name"], "未知机构")
        position = safe_get(survey_data, ["employment_info", "current_position"], "未知岗位")
        city = safe_get(survey_data, ["institution_info", "city"], "未知城市")
        
        # Current date for metadata
        today = datetime.date.today().isoformat()
        
        # --- Build Markdown Content ---
        md_lines = []
        
        # Title
        md_lines.append(f"# 行业调研报告：{inst_name} - {position}")
        md_lines.append(f"> 采集日期: {today} | 城市: {city}")
        md_lines.append("")
        
        # 1. 机构概况
        inst_info = survey_data.get("institution_info", {})
        if isinstance(inst_info, dict):
            md_lines.append("## 1. 机构概况")
            md_lines.append(f"- **名称**: {inst_info.get('name', 'N/A')}")
            md_lines.append(f"- **性质**: {inst_info.get('subject_type', 'N/A')}")
            md_lines.append(f"- **形态**: {inst_info.get('specific_form', 'N/A')}")
            
            is_puhui = inst_info.get('is_puhui')
            puhui_str = '是' if is_puhui is True else '否' if is_puhui is False else 'N/A'
            md_lines.append(f"- **普惠**: {puhui_str}")
            
            service_modes = inst_info.get("service_modes", [])
            if isinstance(service_modes, list):
                md_lines.append(f"- **服务模式**: {', '.join([str(m) for m in service_modes if m])}")
            else:
                md_lines.append(f"- **服务模式**: {str(service_modes)}")
            
            md_lines.append(f"- **规模**: 托位 {inst_info.get('total_capacity', 0)} / 在园 {inst_info.get('current_enrollment', 0)} / 员工 {inst_info.get('staff_count', 0)}")
            md_lines.append("")
        
        # 2. 受访者画像
        personal = survey_data.get("personal_info", {})
        employment = survey_data.get("employment_info", {})
        
        md_lines.append("## 2. 受访者画像")
        if isinstance(personal, dict):
            md_lines.append(f"- **基本信息**: {personal.get('gender', 'N/A')} | {personal.get('education', 'N/A')} ({personal.get('major', 'N/A')})")
        
        if isinstance(employment, dict):
            md_lines.append(f"- **当前岗位**: {employment.get('current_position', 'N/A')}")
            if employment.get('current_position') == "行政人员":
                md_lines.append(f"  - 备注: {employment.get('current_position_other', '')}")
                
            md_lines.append(f"- **薪资范围**: {employment.get('salary_range', 'N/A')}")
            md_lines.append(f"- **换岗频率**: {employment.get('job_change_interval', 'N/A')}")
            
            reasons = employment.get("job_change_reasons", [])
            if isinstance(reasons, list) and reasons:
                md_lines.append(f"- **换岗原因**: {', '.join([str(r) for r in reasons if r])}")
                
            if employment.get("is_kindergarten_transition"):
                md_lines.append(f"- **幼儿园转型**: 是")
                md_lines.append(f"- **转型提升需求**: {employment.get('transition_needs', 'N/A')}")
        md_lines.append("")
        
        # 3. 岗位详情 (核心任务 & 能力要求)
        pos_details = survey_data.get("position_details", {})
        if isinstance(pos_details, dict) and pos_details:
            md_lines.append("## 3. 岗位详情")
            md_lines.append(format_list(pos_details.get("core_tasks", []), "核心工作任务"))
            md_lines.append(format_list(pos_details.get("capability_requirements", []), "能力要求"))
            md_lines.append(format_list(pos_details.get("quality_requirements", []), "素质素养要求"))
            
        # 4. 管理视角 (仅园长/负责人)
        # 宽松匹配岗位名称
        manager_keywords = ["园长", "负责人", "管理"]
        is_manager = False
        if position:
            for kw in manager_keywords:
                if kw in position:
                    is_manager = True
                    break
        
        manager_info = survey_data.get("manager_specific_info", {})
        if isinstance(manager_info, dict) and manager_info and is_manager:
            md_lines.append("## 4. 管理视角")
            
            # 医育结合
            med_edu = manager_info.get("medical_education_combination", {})
            if isinstance(med_edu, dict) and med_edu:
                has_med_content = False
                forms = med_edu.get("forms", [])
                if isinstance(forms, list) and forms:
                    has_med_content = True
                
                if has_med_content or med_edu.get('partner_institutions') or med_edu.get('cooperation_details'):
                    md_lines.append("### 医育结合")
                    if forms:
                        md_lines.append(f"- **开展形式**: {', '.join([str(f) for f in forms if f])}")
                    
                    # 检查是否有外部合作
                    has_coop = False
                    if forms:
                         for f in forms:
                             if "医疗机构" in str(f) or "合作" in str(f):
                                 has_coop = True
                                 break
                    
                    if has_coop or med_edu.get('partner_institutions'):
                        md_lines.append(f"- **合作机构**: {med_edu.get('partner_institutions', 'N/A')}")
                        md_lines.append(f"- **合作详情**: {med_edu.get('cooperation_details', 'N/A')}")
                    md_lines.append("")
            
            # 招聘与培养
            recruit = manager_info.get("recruitment_training", {})
            if isinstance(recruit, dict) and recruit:
                md_lines.append("### 招聘与培养")
                
                # 紧缺岗位
                shortage = recruit.get("shortage_positions", [])
                if isinstance(shortage, list) and shortage:
                    md_lines.append("- **紧缺岗位**:")
                    for item in shortage:
                        if isinstance(item, dict):
                            md_lines.append(f"  - {item.get('position', '未知')}: {item.get('count', 0)}人")
                
                # 学历要求
                edu_reqs = recruit.get("education_requirements", [])
                if isinstance(edu_reqs, list) and edu_reqs:
                    md_lines.append("- **学历要求**:")
                    for item in edu_reqs:
                        if isinstance(item, dict):
                            md_lines.append(f"  - {item.get('position', '未知')}: {item.get('education', 'N/A')}")
                        
                # 证书要求
                cert_reqs = recruit.get("certificate_requirements", [])
                if isinstance(cert_reqs, list) and cert_reqs:
                    md_lines.append("- **证书要求**:")
                    for item in cert_reqs:
                        if isinstance(item, dict):
                            certs = item.get('certificates', [])
                            if isinstance(certs, list):
                                certs_str = ", ".join([str(c) for c in certs])
                            else:
                                certs_str = str(certs)
                            md_lines.append(f"  - {item.get('position', '未知')}: {certs_str}")

                channels = recruit.get('recruitment_channels', [])
                if isinstance(channels, list) and channels:
                     md_lines.append(f"- **招聘渠道**: {', '.join([str(c) for c in channels])}")
                
                factors = recruit.get('priority_factors', [])
                if isinstance(factors, list) and factors:
                     md_lines.append(f"- **优先因素**: {', '.join([str(f) for f in factors])}")
                
                needs = recruit.get('training_needs', [])
                if isinstance(needs, list) and needs:
                     md_lines.append(f"- **培训需求**: {', '.join([str(n) for n in needs])}")
                
                modes = recruit.get('effective_training_modes', [])
                if isinstance(modes, list) and modes:
                     md_lines.append(f"- **有效培养模式**: {', '.join([str(m) for m in modes])}")
                
                if recruit.get('graduate_issues'):
                    md_lines.append(f"- **毕业生问题**: {recruit.get('graduate_issues', 'N/A')}")

        final_markdown = "\n".join(md_lines)
        
        return {
            "markdown_content": final_markdown,
            "document_name": f"调研_{inst_name}_{position}_{today}",
            "raw_data_preview": str(survey_data)[:200] + "..." # Optional debug info
        }

    except Exception as e:
        return {
            "error": str(e),
            "traceback": traceback.format_exc()
        }
