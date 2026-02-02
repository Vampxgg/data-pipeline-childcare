import httpx
import asyncio

async def inspect():
    url = "https://zyyxzy.moe.edu.cn/home/major-register?page=1&year=2025"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    async with httpx.AsyncClient(verify=False) as client:
        resp = await client.get(url, headers=headers)
        with open("debug_moe.html", "w", encoding="utf-8") as f:
            f.write(resp.text)
        print("Saved debug_moe.html")

if __name__ == "__main__":
    asyncio.run(inspect())
