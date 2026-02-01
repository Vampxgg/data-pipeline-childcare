import httpx
import asyncio
import re
import os
import time
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

class MoeMajorScraper:
    def __init__(self, year="2025", concurrency=20):
        self.base_url = "https://zyyxzy.moe.edu.cn/home/major-register"
        self.year = year
        self.concurrency = concurrency
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.output_dir = "data"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
    async def fetch_page(self, client, page):
        """抓取单页并解析"""
        params = {
            "page": page,
            "year": self.year,
            "province": "", # 默认全选
            "school_name": "",
            "school_code": "",
            "major_code": "",
            "major_name": ""
        }
        
        for attempt in range(3):
            try:
                resp = await client.get(self.base_url, params=params, headers=self.headers, timeout=10)
                if resp.status_code == 200:
                    html = resp.text
                    # 正则解析表格行
                    rows = re.findall(r'<tr[^>]*class="table_list"[^>]*>(.*?)</tr>', html, re.DOTALL)
                    data_list = []
                    for row in rows:
                        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
                        clean_cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
                        # 确保列数足够 (省份, 专业代码, 专业名称, 学校标识码, 学校名称, 年限, 备注)
                        if len(clean_cells) >= 7:
                            # 映射为字典以便后续格式化
                            data_list.append({
                                "province": clean_cells[0],
                                "major_code": clean_cells[1],
                                "major_name": clean_cells[2],
                                "school_code": clean_cells[3],
                                "school_name": clean_cells[4],
                                "duration": clean_cells[5],
                                "remark": clean_cells[6]
                            })
                    return data_list
                else:
                    logger.warning(f"Page {page} returned status {resp.status_code}")
                    return []
            except Exception as e:
                if attempt == 2:
                    logger.error(f"Failed to fetch page {page}: {e}")
                await asyncio.sleep(1)
        return []

    async def scrape_and_format(self, start_page=1, end_page=20):
        """并发抓取并直接生成 RAG 格式文本"""
        logger.info(f"Starting scrape for year {self.year}, pages {start_page}-{end_page}...")
        
        sem = asyncio.Semaphore(self.concurrency)
        all_formatted_texts = []
        
        async with httpx.AsyncClient(verify=False) as client:
            tasks = []
            
            async def worker(p):
                async with sem:
                    return await self.fetch_page(client, p)
            
            for page in range(start_page, end_page + 1):
                tasks.append(worker(page))
                
            results = await asyncio.gather(*tasks)
            
            for page_data in results:
                for item in page_data:
                    # 格式化为通过 ^_^ 分割的 RAG 文本块
                    # 字段映射：
                    # 机构名称 -> 学校名称
                    # 别名 -> 备注 (或无)
                    # 统一社会信用代码 -> 学校标识码
                    # 机构类型 -> "高等职业教育专业" (固定或根据需求)
                    # 详细地址 -> 省份 (地址暂无)
                    # 专业信息 -> 专业代码+名称
                    rag_block = (
                        f"机构名称：{item['school_name']}\n"
                        f"省份：{item['province']}\n"
                        f"学校标识码：{item['school_code']}\n"
                        f"开设专业：{item['major_name']} ({item['major_code']})\n"
                        f"修业年限：{item['duration']}\n"
                        f"年份：{self.year}\n"
                        f"备注：{item['remark']}"
                    )
                    all_formatted_texts.append(rag_block)
                    
        # 保存结果
        output_txt = os.path.join(self.output_dir, f"moe_majors_rag_{self.year}.txt")
        # 使用 ^_^ 作为分隔符连接
        final_content = "^_^".join(all_formatted_texts)
        
        with open(output_txt, "w", encoding="utf-8") as f:
            f.write(final_content)
            
        logger.info(f"Scrape completed. Saved {len(all_formatted_texts)} records to {output_txt}")

if __name__ == "__main__":
    # 默认抓取前 100 页作为示例，用户可自行调大参数
    scraper = MoeMajorScraper(year="2025", concurrency=10)
    asyncio.run(scraper.scrape_and_format(start_page=1, end_page=100))
