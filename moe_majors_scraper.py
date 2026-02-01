import sys
import io

# 设置标准输出编码为 utf-8，防止在 Windows 控制台下出现 UnicodeEncodeError
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import requests
import re
import csv
import time
import random
import os

def scrape_moe_majors(year="2025", start_page=1, end_page=5):
    base_url = "https://zyyxzy.moe.edu.cn/home/major-register"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    # 确保 data 目录存在
    output_dir = "data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    csv_file = os.path.join(output_dir, f"moe_majors_{year}.csv")
    
    # 如果是第一页，写入表头
    if start_page == 1:
        with open(csv_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["省份", "专业代码", "专业名称", "学校标识码", "学校名称", "年限", "备注", "年份"])

    for page in range(start_page, end_page + 1):
        print(f"正在抓取第 {page} 页...")
        try:
            params = {"page": page, "year": year}
            resp = requests.get(base_url, params=params, headers=headers, timeout=10)
            
            if resp.status_code == 200:
                html = resp.text
                rows = re.findall(r'<tr[^>]*class="table_list"[^>]*>(.*?)</tr>', html, re.DOTALL)
                
                data_list = []
                for row in rows:
                    cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
                    clean_cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
                    if len(clean_cells) >= 7:
                        clean_cells.append(year) # 添加年份列
                        data_list.append(clean_cells)
                
                # 追加写入文件
                if data_list:
                    with open(csv_file, "a", newline="", encoding="utf-8-sig") as f:
                        writer = csv.writer(f)
                        writer.writerows(data_list)
                    print(f"  - 成功保存 {len(data_list)} 条数据")
                else:
                    print("  - 本页无数据")
            
            # 随机休眠防封
            time.sleep(random.uniform(1, 3))
            
        except Exception as e:
            print(f"  - 第 {page} 页抓取失败: {e}")

if __name__ == "__main__":
    # 示例：抓取 2025 年的前 10 页
    scrape_moe_majors(year="2025", start_page=1, end_page=10)
