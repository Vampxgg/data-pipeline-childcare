import json
import re
from typing import Any, Dict, List, Union
try:
    import json_repair
except ImportError:
    raise ImportError("依赖 'json-repair' 未安装。请在Dify的 '设置' -> '依赖管理' 中添加它。")

# ==============================================================================
# 用户配置区: 在这里定义你期望的JSON输出结构模板！
# ==============================================================================
CONFIG = {
    "expected_json_template": {
        "title": "",
        "text": "",
        "references": {},
        "confirm": -1
    }
}
# ==============================================================================

# V3.1 核心修改点
def _clean_code_block_content(match: re.Match) -> str:
    """
    回调函数：针对性修复/解码单个代码块内部的内容。
    
    V3.1 核心逻辑变更:
    对于 echarts/json 块, 我们不再执行 'json_repair.loads -> json.dumps' 的 "修复再编码" 流程。
    因为这会导致后续整个大JSON编码时, 对已经干净的JSON字符串再次转义, 造成双重转义。

    正确思路是 "解码": 原始的 `content` 是一个被JSON转义过的字符串 (如 `{\\"key\\": \\"value\\"}`),
    我们要做的是把它解码回它在Markdown中应有的样子 (即 `{"key": "value"}`), 也就是去除那层额外的转义。
    """
    lang = match.group(1).lower().strip() if match.group(1) else ""
    content = match.group(2)
    
    # 默认使用原始内容，防止解码失败
    decoded_content = content

    if lang in ['echarts', 'json']:
        try:
            # 关键操作：使用 json.loads 将JSON字面量字符串解码为正常的Python字符串。
            # 例如，输入 `{\\"key\\": \\"value\\"}` (Python中表示为 '{\\"key\\": \\"value\\"}') 
            # `json.loads` 会将其转换为 `{"key": "value"}` (普通的Python字符串)。
            # 这正是我们想要的、可以直接嵌入Markdown的干净代码。
            # 我们需要先把它包装成一个有效的JSON字符串字面量，即 `"` + content + `"`。
            temp_json_string_literal = f'"{content}"'
            decoded_content = json.loads(temp_json_string_literal)
            
        except (json.JSONDecodeError, TypeError):
            # 如果解码失败，说明内容可能不是一个标准的JSON字符串字面量，
            # 可能是因为 LLM 输出不稳定。此时退回使用原始 content 是一种安全的策略。
            # 同时, 执行一次基础的、手动的替换作为降级方案，应对简单情况。
            decoded_content = content.replace('\\n', '\n').replace('\\"', '"').replace("\\'", "'").replace('\\`', '`')
    else:
        # 其他类型代码块的通用清理，逻辑保持不变
        decoded_content = content.replace('\\n', '\n').replace('\\"', '"').replace("\\'", "'")
        decoded_content = decoded_content.replace('\\`', '`')
        
    return f"```{lang}\n{decoded_content}\n```"


def _clean_markdown_string_aggressively(md_string: str) -> str:
    """
    攻击性地清洗Markdown字符串，修复内部代码块。
    (此函数沿用V2版本的强大逻辑, 调用了更新后的回调函数)
    """
    code_block_pattern = re.compile(r'```([\w]*)?\n([\s\S]*?)\n```', re.DOTALL)
    cleaned_string = code_block_pattern.sub(_clean_code_block_content, md_string)
    # 额外清理：有时在代码块之外也可能残留 `\\n` 等，做一次全局清理更保险。
    return cleaned_string.replace('\\n', '\n').replace('\\"', '"')

# # 以下的 main 函数及其他辅助函数无需修改，它们的设计是稳健的。
# # ... (main, _extract_and_clean_field_content 函数保持原样) ...

# def main(raw_input: Any) -> Dict[str, Any]:
#     """
#     超级智能代码执行节点 (V3.1 - 精准去转义版)。
#     """
#     if not isinstance(raw_input, str):
#         input_str = str(raw_input)
#     else:
#         input_str = raw_input

#     if not input_str.strip():
#         raise ValueError("输入为空或只包含空白字符。")

#     # 准备工作
#     template = CONFIG["expected_json_template"]
#     field_names = list(template.keys())
#     cleaned_object = template.copy() # 使用模板作为基础

#     # 核心流程：遍历模板中的字段，去原始字符串中提取并清洗对应内容
#     for i, field_name in enumerate(field_names):
#         next_field_name = field_names[i + 1] if i + 1 < len(field_names) else None

#         if next_field_name:
#             pattern = re.compile(f'"{re.escape(field_name)}"\s*:\s*(.*?)\s*,\s*"{re.escape(next_field_name)}"', re.DOTALL)
#         else:
#             pattern = re.compile(f'"{re.escape(field_name)}"\s*:\s*(.*)\s*}}?$', re.DOTALL)

#         match = pattern.search(input_str)
        
#         if match:
#             content_str = match.group(1).strip()
#             cleaned_content = None
#             if content_str.startswith('"'):
#                 cleaned_str_value = content_str[1:-1]
#                 cleaned_content = _clean_markdown_string_aggressively(cleaned_str_value)
#             elif content_str.startswith('[') or content_str.startswith('{'):
#                 try:
#                     cleaned_content = json_repair.loads(content_str)
#                 except Exception:
#                     cleaned_content = template[field_name]
#             else:
#                 try:
#                     cleaned_content = json.loads(content_str)
#                 except Exception:
#                     cleaned_content = template[field_name]
            
#             cleaned_object[field_name] = cleaned_content
#         else:
#             pass

#     # 返回最终结果
#     output = {
#         "cleaned_object": cleaned_object,
#         "cleaned_string": json.dumps(cleaned_object, ensure_ascii=False, indent=2)
#     }

#     return output

def main(raw_input: Any) -> Dict[str, Any]:
    """
    超级智能代码执行节点 (V3.2 - 最大程度数据还原版)。
    """
    if not isinstance(raw_input, str):
        input_str = str(raw_input)
    else:
        input_str = raw_input

    # --------------------------------------------------------------------------
    # V3.3 新增: 预处理 "双重转义" 或 "JSON字符串内容" 的情况
    # 场景: 输入是 `{\n  \"confirm\": -1 ...` (即一个被转义过的字符串内容)
    # 这种情况下，直接解析会失败，正则也匹配不到(因为有 \" )。
    # 我们尝试将其“反转义”回正常的 JSON 字符串。
    # --------------------------------------------------------------------------
    if '\\"' in input_str:
        try:
            # 技巧: 将其包裹在引号中，构造成一个合法的 JSON 字符串字面量，然后 load 一次
            # 这会自动处理 \n -> 换行, \" -> ", \\ -> \ 等所有转义
            # 注意: 如果 input_str 包含实际的换行符(0x0A)，json.loads 会报错，
            # 所以先简单的把实际换行符转义一下，以防万一
            temp_str = input_str.replace('\n', '\\n').replace('\r', '')
            decoded_str = json.loads(f'"{temp_str}"')
            
            # 如果解码成功，且结果看起来像是一个 JSON 对象/数组
            if isinstance(decoded_str, str) and decoded_str.strip().startswith(('{', '[')):
                input_str = decoded_str
        except Exception:
            # 如果尝试反转义失败，就保持原样，交给后面的逻辑去处理
            pass
    # --------------------------------------------------------------------------

    if not input_str.strip():
        # 如果输入完全是空的，才抛出空模版，这是没办法的事情
        return {"cleaned_object": CONFIG["expected_json_template"], "cleaned_string": "{}"}
    template = CONFIG["expected_json_template"]
    field_names = list(template.keys())
    
    # 【调整】不再直接使用空模板开始，而是先尝试“整体抢救”
    # 策略1：使用 json_repair 对全文进行一次暴力修复和解析
    # 这能解决 90% 的 "缺少逗号"、"未闭合"、"混杂文字" 的情况
    try:
        pre_parsed_data = json_repair.loads(input_str)
        if not isinstance(pre_parsed_data, dict):
            pre_parsed_data = {}
    except Exception:
        pre_parsed_data = {}
    # 【调整】初始化 cleaned_object
    # 如果预解析里有数据，先填进去！而不是让它为空！
    # 这样即使后面的正则全挂了，我们至少保留了 json_repair 抢救回来的数据
    cleaned_object = template.copy()
    for key, default_val in template.items():
        if key in pre_parsed_data:
            val = pre_parsed_data[key]
            # 对抢救回来的字符串类型数据，依然要做一次 Markdown 清洗
            if isinstance(val, str):
                cleaned_object[key] = _clean_markdown_string_aggressively(val)
            else:
                cleaned_object[key] = val
    # 策略2：针对特定字段的“高精度手术” (保留原有逻辑，但作为增强覆盖，而非唯一来源)
    # 这个循环的作用是：如果原始字符串格式非常乱，json_repair 可能漏掉了某些嵌入很深的字段
    # 或者我们需要用正则提取原始字符串并手动 unescape (比 json_repair 更精准的场景)
    for i, field_name in enumerate(field_names):
        next_field_name = field_names[i + 1] if i + 1 < len(field_names) else None
        # 构建严格正则 (原有逻辑)
        if next_field_name:
            pattern = re.compile(f'"{re.escape(field_name)}"\s*:\s*(.*?)\s*,\s*"{re.escape(next_field_name)}"', re.DOTALL)
        else:
            pattern = re.compile(f'"{re.escape(field_name)}"\s*:\s*(.*)\s*}}?$', re.DOTALL)
        match = pattern.search(input_str)
        
        # 【调整】增加兜底正则：如果严格的 "key: val, next_key" 匹配失败，尝试宽松的单字段匹配
        # 这防止因为 next_field 缺失导致当前 field 也匹配不到
        if not match:
             # 【修复】如果 json_repair 已经成功解析了该字段，且严格正则匹配失败（可能是因为字段顺序变了），
             # 则不要尝试宽松匹配！宽松匹配 (fallback_pattern) 在处理包含逗号的字符串/对象时非常脆弱，会导致数据截断。
             if field_name in pre_parsed_data:
                 continue

             # 这是一个宽松的匹配，提取直到逗号或大括号结束的内容
             fallback_pattern = re.compile(f'"{re.escape(field_name)}"\s*:\s*(.*?)(?:\s*,|\s*\}})', re.DOTALL)
             match = fallback_pattern.search(input_str)
        if match:
            content_str = match.group(1).strip()
            
            # 【调整】只有当提取出的内容看起来有效（不为空）时，才去覆盖 cleaned_object
            # 防止正则匹配到了空字符串，把前面 json_repair 辛苦解析出的结果给覆盖没了
            if not content_str:
                continue
            cleaned_content = None
            try:
                if content_str.startswith('"'):
                    cleaned_str_value = content_str[1:-1]
                    cleaned_content = _clean_markdown_string_aggressively(cleaned_str_value)
                elif content_str.startswith('[') or content_str.startswith('{'):
                    cleaned_content = json_repair.loads(content_str)
                else:
                    # 尝试解析普通类型 (int, float, bool)
                    cleaned_content = json.loads(content_str)
                
                # 如果成功解析出了内容，覆盖之前的值
                cleaned_object[field_name] = cleaned_content
            except Exception:
                # 解析失败，保持现状 (即保留 策略1 的结果)
                pass
        else:
            # 【调整】如果正则完全没匹配到
            # 什么都不做！保留 策略1 (json_repair) 已经填入的值
            # 原代码在这里实际上隐式地放弃了寻找，导致保留了空模版的默认值
            pass
    # 返回最终结果
    output = {
        "cleaned_object": cleaned_object,
        "cleaned_string": json.dumps(cleaned_object, ensure_ascii=False, indent=2)
    }
    return output