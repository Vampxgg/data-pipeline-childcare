import httpx
import asyncio
import json
import logging
import sys
import os

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TuoyuScraper:
    def __init__(self):
        self.base_url = "https://tuoyu.cpdrc.org.cn/bapfopm/pub/search/action/queryInfo"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "*/*",
            "Referer": "https://tuoyu.cpdrc.org.cn/"
        }
        self.output_dir = "data"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    async def fetch_page(self, page_num, page_size=10, keyword=""):
        """
        获取单页数据
        """
        params = {
            "pageNum": page_num,
            "pageSize": page_size,
            "key": keyword
        }
        
        try:
            async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
                response = await client.get(self.base_url, params=params, headers=self.headers)
                response.raise_for_status()
                
                data = response.json()
                if data.get("code") == "000000":
                    return data.get("responseData", {})
                else:
                    logger.error(f"API Error: {data.get('message')}")
                    return None
        except Exception as e:
            logger.error(f"Request failed for page {page_num}: {e}")
            return None

    async def scrape_all(self, start_page=1, max_pages=None):
        """
        爬取所有数据
        """
        # 先获取第一页以确定总页数
        first_page_data = await self.fetch_page(1)
        if not first_page_data:
            logger.error("Failed to fetch first page. Exiting.")
            return

        total_record = first_page_data.get("totalRecord", 0)
        total_page = first_page_data.get("totalPage", 0)
        
        logger.info(f"Total records: {total_record}, Total pages: {total_page}")
        
        if max_pages:
            total_page = min(total_page, max_pages)
            
        all_data = []
        all_data.extend(first_page_data.get("dataList", []))
        
        # 并发获取剩余页面
        tasks = []
        # 限制并发数
        sem = asyncio.Semaphore(10) 
        
        async def fetch_with_sem(page):
            async with sem:
                logger.info(f"Fetching page {page}/{total_page}")
                data = await self.fetch_page(page)
                if data:
                    return data.get("dataList", [])
                return []

        for page in range(start_page + 1, total_page + 1):
            tasks.append(fetch_with_sem(page))
            
        results = await asyncio.gather(*tasks)
        
        for res in results:
            all_data.extend(res)
            
        # 保存数据
        output_file = os.path.join(self.output_dir, "tuoyu_data.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Successfully scraped {len(all_data)} records. Saved to {output_file}")
        return all_data

if __name__ == "__main__":
    scraper = TuoyuScraper()
    asyncio.run(scraper.scrape_all(max_pages=5)) # 测试爬取前5页
