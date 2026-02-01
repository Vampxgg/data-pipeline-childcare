import json
import traceback
import re

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
        # 处理类似 "Here is the JSON: { ... }" 的情况
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

def find_scenes(data):
    """
    在数据结构中递归查找看起来像场景列表的对象。
    特征：是一个列表，且列表项包含 'type', 'duration', 'estimated_duration_seconds' 等关键字段。
    """
    if isinstance(data, list):
        # 检查列表本身是否就是场景列表
        if data and isinstance(data[0], dict):
            # 检查关键字段特征
            keys = data[0].keys()
            if any(k in keys for k in ["type", "estimated_duration_seconds", "scene_knowledge", "subtitles"]):
                return data
        return []
    
    if isinstance(data, dict):
        # 1. 直接匹配常见键名
        for key in ["scenes", "Scenes", "script", "content"]:
            if key in data and isinstance(data[key], list):
                return find_scenes(data[key]) # 递归检查确认
        
        # 2. 遍历所有值进行查找 (深度优先，只找一层或两层以防性能问题，这里全遍历)
        # 优先查找键名包含 'scene' 的
        for key, value in data.items():
            if "scene" in key.lower() and isinstance(value, list):
                found = find_scenes(value)
                if found: return found

        # 3. 普适性递归查找
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                found = find_scenes(value)
                if found:
                    return found
                     
    return []

def format_time(seconds):
    """
    Format seconds into MM:SS format.
    """
    try:
        seconds = float(seconds)
        m = int(seconds // 60)
        s = int(seconds % 60)
        return f"{m:02d}:{s:02d}"
    except:
        return "00:00"

def extract_outline(script_data):
    """
    Extract outline from script data.
    Target format: List of objects with id, title, description, duration, time_range
    """
    outline = []
    current_time = 0.0
    
    # 使用智能查找获取场景列表
    scenes = find_scenes(script_data)
    
    if not scenes:
        # 如果找不到场景，尝试把整个输入当做单个场景（极端兜底）
        if isinstance(script_data, dict) and "estimated_duration_seconds" in script_data:
            scenes = [script_data]
        else:
            return []

    for i, scene in enumerate(scenes):
        duration = scene.get("estimated_duration_seconds", 0)
        # 兼容 duration 字段
        if duration == 0:
            duration = scene.get("duration", 0)
            
        start_time = current_time
        end_time = current_time + float(duration)
        
        # Determine title and description based on scene type
        title = scene.get("title", "")
        if not title:
            # Fallback for normal scenes if title is missing
            scene_type = scene.get("type", "normal")
            if scene_type == "normal":
                title = f"场景 {i+1}"
            else:
                title = str(scene_type).capitalize()
                
        description = scene.get("scene_knowledge", "")
        if not description:
             description = scene.get("target", "")
        if not description:
             description = scene.get("subtitle", "") # For cover/ending
        if not description:
             # 尝试从 subtitles 拼接一点内容作为描述
             subs = scene.get("subtitles", [])
             if subs and isinstance(subs, list) and len(subs) > 0:
                 first_sub = subs[0]
                 if isinstance(first_sub, dict):
                     description = first_sub.get("text", "")[:50]
            
        outline_item = {
            "id": scene.get("id", f"scene_{i}"),
            "index": i + 1,
            "title": title,
            "description": description,
            "duration": f"{duration}s",
            "startTime": format_time(start_time),
            "endTime": format_time(end_time),
            "raw_duration": duration
        }
        outline.append(outline_item)
        current_time = end_time
        
    return outline

def extract_subtitles(script_data):
    """
    Extract subtitles from script data.
    Target format: List of objects with id, startTime, endTime, text
    """
    subtitles_output = []
    
    # 使用智能查找获取场景列表
    scenes = find_scenes(script_data)
    
    # 全局时间偏移量
    current_scene_start_time = 0.0
    
    for scene in scenes:
        scene_duration = float(scene.get("estimated_duration_seconds", 0) or scene.get("duration", 0))
        
        # Check for subtitles list in scene
        subs = scene.get("subtitles", [])
        if subs:
            for sub in subs:
                raw_start = float(sub.get("start_time_seconds", 0) or sub.get("startTime", 0))
                raw_end = float(sub.get("end_time_seconds", 0) or sub.get("endTime", 0))
                
                # 智能判断：是绝对时间还是相对时间？
                # 如果原始时间值 >= 当前场景的起始时间，假设它是绝对时间
                # 否则，假设它是相对时间（相对于场景开始）
                if raw_start >= current_scene_start_time:
                    abs_start = raw_start
                    # 如果 start 是绝对的，end 通常也是绝对的
                    abs_end = raw_end
                else:
                    abs_start = current_scene_start_time + raw_start
                    # 如果 start 是相对的，end 通常也是相对的，但为了保险，计算差值
                    duration = raw_end - raw_start
                    if duration > 0:
                        abs_end = abs_start + duration
                    else:
                        # 如果无法计算 duration，尝试直接用 raw_end (如果它也是相对的)
                        abs_end = current_scene_start_time + raw_end

                subtitles_output.append({
                    "id": sub.get("id"),
                    "startTime": format_time(abs_start),
                    "endTime": format_time(abs_end),
                    "text": sub.get("text", ""),
                    "raw_startTime": abs_start
                })
        
        current_scene_start_time += scene_duration
    
    # Sort by start time just in case
    subtitles_output.sort(key=lambda x: x.get("raw_startTime", 0))
    
    # Remove raw keys if not needed for frontend, or keep them if useful
    for item in subtitles_output:
        item.pop("raw_startTime", None)
        
    return subtitles_output

def extract_data_sources(resource_data):
    """
    Extract data sources from resource data.
    Target format: List of objects with type, title, url, description/snippet
    """
    sources_output = []
    
    # Resource data might be a list of training/generation results
    if isinstance(resource_data, list):
        for item in resource_data:
            # Navigate deep structure: web_data -> comprehensive_data -> all_source_list
            web_data = item.get("web_data", {})
            comp_data = web_data.get("comprehensive_data", {})
            source_list = comp_data.get("all_source_list", [])
            
            for source in source_list:
                # Map source type to frontend friendly type
                src_type = source.get("type", "WEB").upper()
                if "PDF" in src_type or "FILE" in src_type:
                    src_type = "PDF"
                elif "WEB" in src_type:
                    src_type = "WEB"
                
                sources_output.append({
                    "type": src_type,
                    "title": source.get("title", "Untitled Source"),
                    "url": source.get("url", ""),
                    "description": source.get("snippet", "") or source.get("content", "")[:200] + "...",
                    "source_name": source.get("source", "")
                })
                
    return sources_output

def main(script_input, resource_input):
    """
    Main entry point for Dify.
    """
    try:
        # 1. Parse Inputs
        script_data = safe_parse_json(script_input)
        resource_data = safe_parse_json(resource_input)
        
        # 2. Extract Data
        outline = extract_outline(script_data)
        subtitles = extract_subtitles(script_data)
        data_sources = extract_data_sources(resource_data)
        
        # 3. Construct Final Output
        result = {
            "outline": outline,
            "subtitles": subtitles,
            "data_sources": data_sources
        }
        
        return {
            "result": result
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "traceback": traceback.format_exc()
        }

if __name__ == "__main__":
    # Test with dummy data if run directly
    print("This script is designed to be run within Dify or imported.")
