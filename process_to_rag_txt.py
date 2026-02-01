
import json
import os
from collections import defaultdict
import re

def process_json_to_split_txt_by_year_month(input_json_path, output_dir):
    """
    按照年月 (YYYY-MM) 将数据拆分为多个 TXT 文件。
    每个文件命名格式：托育机构备案信息_{year_month}.txt
    内容分隔符使用 ^_^
    """
    if not os.path.exists(input_json_path):
        print(f"错误: 找不到输入文件 {input_json_path}")
        return

    # 确保输出目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"正在读取 {input_json_path} ...")
    with open(input_json_path, 'r', encoding='utf-8') as f:
        try:
            raw_data = json.load(f)
        except Exception as e:
            print(f"错误: 解析 JSON 失败 - {e}")
            return

    # 按年月分组
    grouped_data = defaultdict(list)
    
    for item in raw_data:
        # 获取必须的字段
        name = str(item.get('institution_name', '')).replace("^_^", "")
        other_name = item.get('institution_other_name', '无')
        credit_code = item.get('credit_code', '无')
        ins_type = item.get('institution_type', '未分类')
        address = item.get('address', '地址暂无')
        finished_time = item.get('finished_time', '')
        
        zoning_name = item.get('zoning_name', '')
        zone_code = item.get('zoning_code', '')

        # 提取年月逻辑 (YYYY-MM)
        year_month = "unknown"
        if finished_time:
            # 统一分隔符
            ft_clean = finished_time.strip().replace('/', '-')
            # 尝试匹配 YYYY-MM
            match = re.search(r'(\d{4}-\d{2})', ft_clean)
            if match:
                year_month = match.group(1)
            else:
                # 尝试匹配无间隔的 YYYYMM (虽然少见但为了健壮)
                match_compact = re.search(r'(\d{4}\d{2})', ft_clean)
                if match_compact:
                    year_month = match_compact.group(1)[:4] + "-" + match_compact.group(1)[4:]
                else:
                    year_month = "unknown"
        else:
            year_month = "unknown"

        # 格式化文本
        rag_text = (
            f"机构名称：{name}\n"
            f"别名：{other_name}\n"
            f"统一社会信用代码：{credit_code}\n"
            f"机构类型：{ins_type}\n"
            f"详细地址：{zoning_name}{address}\n"
            f"备案及完成时间：{finished_time if finished_time else '未知'}\n"
            f"区域编号：{zone_code}"
        )
        
        grouped_data[year_month].append(rag_text)

    # 写入文件
    print(f"共识别出 {len(grouped_data)} 个年月分组，开始写入文件...")
    
    count = 0
    for ym, texts in grouped_data.items():
        filename = f"托育机构备案信息_{ym}.txt"
        filepath = os.path.join(output_dir, filename)
        
        file_content = "^_^".join(texts)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(file_content)
        count += 1
        
    print(f"处理完成！已生成 {count} 个文件在 '{output_dir}' 目录下。")

if __name__ == "__main__":
    # 配置路径
    input_file = os.path.join("data", "tuoyu_data_fast.json")
    output_directory = os.path.join("data", "split_txts_by_year_month")
    
    process_json_to_split_txt_by_year_month(input_file, output_directory)
