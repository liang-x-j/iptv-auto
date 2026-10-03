import asyncio
import time
import aiohttp

# 优质公开源聚合地址（可继续添加）
SOURCE_URLS = [
    "https://iptv-org.github.io/iptv/countries/cn.m3u",
]

OUTPUT_FILE = "live.m3u"
CHECK_TIMEOUT = 3.0      # 单个源测速超时（秒）
MAX_CONCURRENCY = 50     # 并发测速数


async def fetch_text(session, url, timeout=15):
    try:
        async with session.get(url, timeout=timeout) as resp:
            if resp.status == 200:
                return await resp.text()
    except Exception as e:
        print(f"拉取失败 {url}: {e}")
    return None


def parse_m3u(text):
    """解析 m3u 文本，完整保留原始 EXTINF 行
    （含 tvg-id / tvg-logo / group-title 等属性，台标和分组不丢失）。
    返回 [(extinf_line, url), ...]"""
    channels = []
    pending_extinf = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#EXTINF"):
            pending_extinf = line
        elif line.startswith("#"):
            continue
        elif line.startswith("http"):
            channels.append((pending_extinf or "#EXTINF:-1,", line))
            pending_extinf = None
    return channels


async def test_stream(session, url):
    """浅层保活检查：能建连并读到数据即算存活"""
    try:
        start = time.time()
        async with session.get(url, timeout=CHECK_TIMEOUT) as resp:
            if resp.status == 200:
                chunk = await resp.content.read(1024)
                if chunk:
                    return True, time.time() - start
    except Exception:
        pass
    return False, 0


async def main():
    print("开始执行抓取与测速保活任务...")
    async with aiohttp.ClientSession(
        headers={"User-Agent": "Mozilla/5.0"}
    ) as session:
        # 1. 抓取
        texts = await asyncio.gather(
            *[fetch_text(session, u) for u in SOURCE_URLS]
        )
        all_channels = []
        for text in texts:
            if text:
                all_channels.extend(parse_m3u(text))
        print(f"共抓取到原始频道: {len(all_channels)} 个")

        # 2. 按 URL 去重
        seen = set()
        unique = []
        for extinf, url in all_channels:
            if url not in seen:
                seen.add(url)
                unique.append((extinf, url))
        print(f"去重后: {len(unique)} 个，开始异步测速...")

        # 3. 并发测速
        sem = asyncio.Semaphore(MAX_CONCURRENCY)

        async def bounded(extinf, url):
            async with sem:
                ok, latency = await test_stream(session, url)
                return ok, latency, extinf, url

        results = await asyncio.gather(
            *[bounded(e, u) for e, u in unique]
        )
        valid = [(lat, e, u) for ok, lat, e, u in results if ok]
        valid.sort(key=lambda x: x[0])  # 按延迟排序

        # 4. 生成 live.m3u（保留原始 EXTINF 行）
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for _, extinf, url in valid:
                f.write(extinf + "\n")
                f.write(url + "\n")

        print(f"任务完成！成功存活: {len(valid)} 个，已写入 {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
