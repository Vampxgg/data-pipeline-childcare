import httpx
import asyncio
import json
import logging
import time
import os
from typing import List, Dict, Any

# 设置更详细的日志以监控速度
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - [%(levelname)s] - %(message)s'
)
logger = logging.getLogger(__name__)

class FastTuoyuScraper:
    def __init__(self, concurrency: int = 100):
        self.base_url = "https://tuoyu.cpdrc.org.cn/bapfopm/pub/search/action/queryInfo"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "*/*",
            "Referer": "https://tuoyu.cpdrc.org.cn/",
            "Connection": "keep-alive" # 保持连接以复用
        }
        self.output_dir = "data"
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    async def fetch_page(self, client: httpx.AsyncClient, page_num: int, page_size: int = 10) -> List[Dict]:
        """抓取单页，带重试逻辑"""
        params = {
            "pageNum": page_num,
            "pageSize": page_size,
            "key": ""
        }
        
        async with self.semaphore:
            for attempt in range(3): # 最高3次重试
                try:
                    # 禁用 SSL 验证以避免证书握手开销（针对该政府站点）
                    response = await client.get(
                        self.base_url, 
                        params=params, 
                        headers=self.headers,
                        timeout=15.0 
                    )
                    response.raise_for_status()
                    data = response.json()
                    if data.get("code") == "000000":
                        return data.get("responseData", {}).get("dataList", [])
                    return []
                except Exception as e:
                    if attempt == 2:
                        logger.error(f"Page {page_num} failed after 3 attempts: {e}")
                    await asyncio.sleep(0.5 * (attempt + 1)) # 指数退避
            return []

    async def scrape_all(self, start_page: int = 1, max_pages: int = None):
        start_time = time.time()
        
        # 核心优化：连接池 (Limits) 设置
        # 100 并发，Max Keepalive 允许长连接复用
        limits = httpx.Limits(max_connections=self.concurrency, max_keepalive_connections=50)
        
        async with httpx.AsyncClient(verify=False, limits=limits, headers=self.headers) as client:
            
            # 先确认总量
            logger.info("Initializing... fetching total page count.")
            try:
                resp = await client.get(self.base_url, params={"pageNum": 1, "pageSize": 10})
                meta = resp.json().get("responseData", {})
                total_page = meta.get("totalPage", 0)
                total_record = meta.get("totalRecord", 0)
            except Exception as e:
                logger.error(f"Failed to get metadata: {e}")
                return

            if max_pages:
                total_page = min(total_page, max_pages)

            logger.info(f"Targeting {total_page} pages ({total_record} records) with concurrency {self.concurrency}")

            # 3. 创建并发任务
            tasks = []
            for page in range(start_page, total_page + 1):
                tasks.append(self.fetch_page(client, page))

            # 4. 执行
            results = await asyncio.gather(*tasks)
            
            # 5. 合并
            all_data = []
            for res in results:
                if res:
                    all_data.extend(res)

            end_time = time.time()
            duration = end_time - start_time
            
            # 6. 持久化数据
            output_file = os.path.join(self.output_dir, "tuoyu_data_fast.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)

            logger.info(f"--- Statistics ---")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Pages Scraped: {len(tasks)}")
            logger.info(f"Total Records: {len(all_data)}")
            logger.info(f"Average Speed: {len(tasks)/duration:.2f} pages/sec")
            logger.info(f"Saved to: {output_file}")
            
            # 理论计算：6272页 / 180秒 = 34.8 pages/sec
            # 当前配置在常规宽带下应能达到 40-70 pages/sec，预计仅需 1.5 - 2 分钟。

if __name__ == "__main__":
    # 如果 100 并发被封 IP，请降低到 50
    scraper = FastTuoyuScraper(concurrency=20)
    asyncio.run(scraper.scrape_all(max_pages=6272))
