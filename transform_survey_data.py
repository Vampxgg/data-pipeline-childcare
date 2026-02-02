import re
import json
import os

def parse_ts_config(file_path):
    """
    从 formConfig.ts 中提取字段映射关系
    返回格式: { 'field_key': { 'type': 'options/matrix', 'map': {...} } }
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    mappings = {}
    
    # 1. 查找所有定义的 key 位置
    # 匹配 key: 'fieldName'
    key_pattern = re.compile(r"key:\s*['\"]([^'\"]+)['\"]")
    keys = []
    for match in key_pattern.finditer(content):
        keys.append((match.group(1), match.start()))
    
    # 2. 遍历每个 key，在其后方寻找 options 或 matrix 配置
    for i, (key_name, start_pos) in enumerate(keys):
        # 搜索范围：当前 key 到下一个 key 之间，或者文件末尾
        end_pos = keys[i+1][1] if i + 1 < len(keys) else len(content)
        block_content = content[start_pos:end_pos]
        
        # --- 情况 A: 普通选项 (Select/Radio/Checkbox) ---
        # 匹配 options: [ ... ]
        options_match = re.search(r"options:\s*\[(.*?)\]", block_content, re.DOTALL)
        if options_match:
            options_str = options_match.group(1)
            # 提取 { label: '...', value: '...' }
            # 兼容 value 是字符串或数字
            opt_map = {}
            # 正则解释：找 label:'xxx' ... value:'xxx' 或者 value: 123
            items = re.finditer(r"label:\s*['\"]([^'\"]+)['\"].*?value:\s*(['\"]?)([^'\"}\s,]+)\2", options_str, re.DOTALL)
            for item in items:
                label = item.group(1)
                val = item.group(3)
                opt_map[val] = label
            
            if opt_map:
                mappings[key_name] = {'type': 'options', 'map': opt_map}
                continue

        # --- 情况 B: 矩阵题 (Matrix) ---
        # 匹配 rows: [...] 和 columns: [...]
        rows_match = re.search(r"rows:\s*\[(.*?)\]", block_content, re.DOTALL)
        cols_match = re.search(r"columns:\s*\[(.*?)\]", block_content, re.DOTALL)
        
        if rows_match and cols_match:
            row_map = {}
            col_map = {}
            
            # 提取行映射
            for item in re.finditer(r"label:\s*['\"]([^'\"]+)['\"].*?value:\s*(['\"]?)([^'\"}\s,]+)\2", rows_match.group(1), re.DOTALL):
                row_map[item.group(3)] = item.group(1)
            
            # 提取列映射
            for item in re.finditer(r"label:\s*['\"]([^'\"]+)['\"].*?value:\s*(['\"]?)([^'\"}\s,]+)\2", cols_match.group(1), re.DOTALL):
                col_map[item.group(3)] = item.group(1)
                
            mappings[key_name] = {'type': 'matrix', 'rows': row_map, 'cols': col_map}

    return mappings

def transform_json(data, mappings):
    """
    根据映射表转换 JSON 数据
    """
    new_data = data.copy()
    
    for key, value in data.items():
        # 如果该字段在配置中有映射关系
        if key in mappings:
            config = mappings[key]
            
            # 处理普通选项
            if config['type'] == 'options':
                mapping = config['map']
                if isinstance(value, list):
                    # 多选：转换列表中的每一项
                    new_data[key] = [mapping.get(str(v), v) for v in value]
                else:
                    # 单选
                    new_data[key] = mapping.get(str(value), value)
            
            # 处理矩阵题
            elif config['type'] == 'matrix':
                if isinstance(value, dict):
                    new_matrix = {}
                    for row_key, col_key in value.items():
                        # 转换行 Key (如 item1 -> 热爱本职...)
                        new_r = config['rows'].get(str(row_key), row_key)
                        # 转换列 Value (如 3 -> 一般)
                        new_c = config['cols'].get(str(col_key), col_key)
                        new_matrix[new_r] = new_c
                    new_data[key] = new_matrix
                    
    return new_data

if __name__ == "__main__":
    base_dir = r"e:/Vampxgg/数据管道pipeline/托育/data"
    ts_path = os.path.join(base_dir, "formConfig.ts")
    json_path = os.path.join(base_dir, "demo2.json")
    output_path = os.path.join(base_dir, "demo2_chinese.json")

    import sys
    # 强制设置标准输出编码为 utf-8 (解决 Windows 控制台打印中文报错)
    sys.stdout.reconfigure(encoding='utf-8')

    print(f"Parsing config: {ts_path} ...")
    mappings = parse_ts_config(ts_path)
    print(f"Parsing complete. Captured mappings for {len(mappings)} fields.")
    
    # 打印一些示例映射以供验证
    if 'orgNature' in mappings:
        print("Example mapping (orgNature):", mappings['orgNature']['map'])

    print(f"\nTransforming data: {json_path} ...")
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        transformed_data = transform_json(data, mappings)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(transformed_data, f, ensure_ascii=False, indent=2)
            
        print(f"Success! Result saved to: {output_path}")
        
        # 打印部分结果预览
        print("\n--- Result Preview ---")
        preview_keys = ['orgNature', 'serviceMode', 'competency_matrix']
        for k in preview_keys:
            if k in transformed_data:
                print(f"{k}: {json.dumps(transformed_data[k], ensure_ascii=False)}")
                
    except Exception as e:
        print(f"发生错误: {e}")
