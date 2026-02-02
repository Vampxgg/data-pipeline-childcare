import httpx
import asyncio
import re
import os
import time
import logging
import random
from collections import defaultdict

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

class MoeMajorScraper:
    def __init__(self, concurrency=5): # 默认并发降低到 5
        self.base_url = "https://zyyxzy.moe.edu.cn/home/major-register"
        self.concurrency = concurrency
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive" # 显式设置 keep-alive
        }
        self.output_dir = os.path.join("data", "moe_majors_split")
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
    async def fetch_page(self, client, year, page):
        """抓取单页并解析"""
        params = {
            "page": page,
            "year": year,
            "province": "", 
            "school_name": "",
            "school_code": "",
            "major_code": "",
            "major_name": ""
        }
        
        # 指数退避重试：5次 -> 2s, 4s, 8s, 16s, 32s
        for attempt in range(5):
            try:
                # 随机延迟，模拟真人行为
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
                resp = await client.get(self.base_url, params=params, headers=self.headers, timeout=30)
                
                if resp.status_code == 200:
                    html = resp.text
                    if "频繁" in html or "禁止" in html:
                        logger.warning(f"[{year}] Page {page} detected BLOCKING keywords. Sleeping...")
                        await asyncio.sleep(10 + attempt * 5)
                        continue
                        
                    rows = re.findall(r'<tr[^>]*class="table_list"[^>]*>(.*?)</tr>', html, re.DOTALL)
                    data_list = []
                    for row in rows:
                        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
                        clean_cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
                        
                        if len(clean_cells) >= 7:
                            data_list.append({
                                "province": clean_cells[0],
                                "major_code": clean_cells[1],
                                "major_name": clean_cells[2],
                                "school_code": clean_cells[3],
                                "school_name": clean_cells[4],
                                "duration": clean_cells[5],
                                "remark": clean_cells[6]
                            })
                    
                    if not data_list and "table_list" not in html:
                         # 可能是空页或者结构变了，也可能是被软封禁
                         logger.warning(f"[{year}] Page {page} retrieved but no data found.")
                         
                    return data_list, html
                elif resp.status_code == 403 or resp.status_code == 429:
                    logger.warning(f"[{year}] Page {page} blocked ({resp.status_code}). Backing off...")
                    await asyncio.sleep(10 + attempt * 10) # 遇到封禁多睡会儿
                else:
                    logger.warning(f"[{year}] Page {page} status {resp.status_code}")
                    await asyncio.sleep(2)
            except Exception as e:
                # 捕获连接重置、超时等
                if attempt >= 3:
                     logger.error(f"[{year}] Error fetching page {page} (Attempt {attempt+1}): {e}")
                await asyncio.sleep(2 * (attempt + 1))
                
        return [], ""

    async def get_total_pages(self, client, year):
        """获取某年份的总页数"""
        try:
            _, html = await self.fetch_page(client, year, 1)
            if not html:
                return 0
            
            match = re.search(r'page=(\d+)&[^"]*">末页', html)
            if match:
                return int(match.group(1))
            
            if "table_list" in html:
                return 1
                
            return 0
        except Exception as e:
            logger.error(f"[{year}] Error getting total parsing: {e}")
            return 0

    async def scrape_year(self, year):
        """抓取某一年的所有数据并按省份保存"""
        logger.info(f"=== Starting scrape for Year {year} ===")
        
        # 强制低并发
        sem = asyncio.Semaphore(self.concurrency)
        
        # 创建 client 时不设置过高的连接数，使用默认即可，避免被防火墙识别特征
        async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
            total_pages = await self.get_total_pages(client, year)
            if total_pages == 0:
                logger.warning(f"[{year}] No pages found or init failed.")
                return

            logger.info(f"[{year}] Total pages: {total_pages}. Processing with concurrency {self.concurrency}...")
            
            tasks = []
            async def worker(p):
                async with sem:
                    data, _ = await self.fetch_page(client, year, p)
                    return data

            for page in range(1, total_pages + 1):
                tasks.append(worker(page))
            
            # 使用 gather
            results = await asyncio.gather(*tasks)
            
            data_by_province = defaultdict(list)
            total_count = 0
            
            for page_data in results:
                if not page_data:
                    continue
                for item in page_data:
                    province = item['province'].strip()
                    if not province:
                        province = "其他"
                        
                    rag_block = (
                        f"机构名称：{item['school_name']}\n"
                        f"省份：{province}\n"
                        f"学校标识码：{item['school_code']}\n"
                        f"开设专业：{item['major_name']} ({item['major_code']})\n"
                        f"修业年限：{item['duration']}\n"
                        f"年份：{year}\n"
                        f"备注：{item['remark']}"
                    )
                    data_by_province[province].append(rag_block)
                    total_count += 1

            if total_count == 0:
                logger.warning(f"[{year}] No data extracted (Wait, maybe IP blocked?).")
                return

            for province, texts in data_by_province.items():
                safe_province = re.sub(r'[\\/*?:"<>|]', "", province)
                filename = f"moe_majors_{safe_province}_{year}.txt"
                filepath = os.path.join(self.output_dir, filename)
                
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write("^_^".join(texts))
                
            logger.info(f"[{year}] Completed. {total_count} records saved.")

    async def run_all_years(self, years):
        for year in years:
            await self.scrape_year(str(year))
            # 年份之间进行长休眠
            logger.info("Sleeping 10s between years...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    years_to_scrape = range(2020, 2012, -1) 
    
    # 降低并发到 5
    # 虽然慢，但是稳
    scraper = MoeMajorScraper(concurrency=5)
    asyncio.run(scraper.run_all_years(years_to_scrape))
