import gradio as gr
import asyncio
import math
import random
import polars as pl
from curl_cffi.requests import AsyncSession

# 控制瞬时压强
SEMAPHORE = asyncio.Semaphore(5)

# 核心武器：东方财富 CDN 边缘节点池
EASTMONEY_NODES = [
    "push2.eastmoney.com",
    "11.push2.eastmoney.com",
    "44.push2.eastmoney.com",
    "82.push2.eastmoney.com",
    "99.push2.eastmoney.com",
    "28.push2.eastmoney.com"
]

# 板块类型映射字典
BOARD_TYPES = {
    "行业": "m:90 t:2",
    "概念": "m:90 t:3",
    "地域": "m:90 t:1"
}

def get_default_params(fs_value):
    return {
        "pn": "1", "pz": "100", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2", "invt": "2", "fid": "f3",
        "fs": fs_value,
        "fields": "f12,f14,f2,f3,f62"
    }

async def fetch_with_retry(session, page_num, board_name, fs_value, max_retries=5):
    """带节点轮询、协议降级(HTTP)和指数退避的强健抓取函数"""
    params = get_default_params(fs_value)
    params["pn"] = str(page_num)
    
    # 注入极其逼真的旧版/普通浏览器全套 Header
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Referer": "http://quote.eastmoney.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for attempt in range(max_retries):
        async with SEMAPHORE:
            node = random.choice(EASTMONEY_NODES)
            # 【核心修改】：使用 http:// 绕过 TLS 握手层风控！
            url = f"http://{node}/api/qt/clist/get"
            
            try:
                # 稍微增加一点延迟，规避并发检测
                await asyncio.sleep(random.uniform(0.3, 1.0))
                
                # impersonate 依然保留，用于底层的 HTTP/2 帧控制
                resp = await session.get(url, params=params, headers=headers, timeout=10)
                
                if resp.status_code == 200:
                    json_data = resp.json()
                    data_dict = json_data.get("data", {})
                    if data_dict:
                        return {"type": board_name, "data": data_dict}
                    return None
                else:
                    print(f"[-] HTTP 状态码异常: {resp.status_code}")
                
            except Exception as e:
                wait_time = (attempt + 1) * 2
                # 【核心修改】：打印 repr(e) 暴露出底层的真实报错原因 (Timeout? Reset? DNS?)
                print(f"[!] {board_name} 第 {page_num} 页 (节点 {node}) 抓取失败: {repr(e)}。等待 {wait_time}s 重试...")
                await asyncio.sleep(wait_time)
                
    return None

async def fetch_single_board_type(session, board_name, fs_value):
    """拉取单个类型板块的所有分页"""
    print(f"[*] 开始探测【{board_name}】板块总数...")
    first_page_res = await fetch_with_retry(session, 1, board_name, fs_value)
    
    if not first_page_res or not first_page_res.get("data"):
        return []

    data_dict = first_page_res["data"]
    total_count = data_dict.get("total", 0)
    first_page_data = data_dict.get("diff", [])
    
    if total_count == 0:
        return []

    page_size = 100
    total_pages = math.ceil(total_count / page_size)
    print(f"[+] 【{board_name}】共计 {total_count} 个，分 {total_pages} 页拉取。")

    all_data = []
    for item in first_page_data:
        item["board_type"] = board_name
    all_data.extend(first_page_data)

    if total_pages > 1:
        tasks = [fetch_with_retry(session, p, board_name, fs_value) for p in range(2, total_pages + 1)]
        remaining_pages = await asyncio.gather(*tasks)
        
        for page_res in remaining_pages:
            if page_res and "data" in page_res and page_res["data"].get("diff"):
                page_diff = page_res["data"]["diff"]
                for item in page_diff:
                    item["board_type"] = board_name
                all_data.extend(page_diff)

    return all_data

async def core_scraping_logic():
    """核心控制逻辑，被 Gradio 按钮调用"""
    async with AsyncSession(impersonate="chrome120") as session:
        all_boards_data = []
        for b_name, fs_val in BOARD_TYPES.items():
            board_data = await fetch_single_board_type(session, b_name, fs_val)
            all_boards_data.extend(board_data)
            await asyncio.sleep(1)

        if not all_boards_data:
            return None, None

        # Polars 数据清洗
        df = pl.DataFrame(all_boards_data).select([
            pl.col("board_type").alias("板块大类"),
            pl.col("f12").alias("板块代码"),
            pl.col("f14").alias("板块名称"),
            (pl.col("f2") / 100).alias("最新点位"),
            pl.col("f3").alias("涨跌幅(%)"),
            (pl.col("f62") / 1e8).alias("主力净流入(亿元)")
        ]).filter(pl.col("板块代码") != "-").sort(["板块大类", "涨跌幅(%)"], descending=[False, True])
        
        file_path = "all_boards_merged.parquet"
        df.write_parquet(file_path)
        
        # 转换为 Pandas 供 Gradio 表格展示
        return df.to_pandas(), file_path

# --- Gradio UI 界面 ---
def start_scraping():
    # 在非异步的 Gradio 环境中运行 asyncio 事件循环
    df_pandas, file_path = asyncio.run(core_scraping_logic())
    if df_pandas is None:
        return None, None, "抓取失败：被防火墙拦截或无数据。"
    return df_pandas, file_path, f"抓取成功！共获取 {len(df_pandas)} 个板块数据。"

with gr.Blocks(title="东财量化数据中台", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# 🚀 东方财富 - 三大板块全量抓取终端")
    gr.Markdown("当前运行环境：阿里云魔塔机房。拥有国内骨干网直连优势，完美绕过海外 IP 限制。")
    
    with gr.Row():
        btn = gr.Button("点击开始全量抓取", variant="primary")
        status_text = gr.Textbox(label="系统运行状态", interactive=False)
        
    with gr.Row():
        data_table = gr.Dataframe(label="数据流预览 (支持点击表头排序)", max_height=500)
        
    with gr.Row():
        download_btn = gr.File(label="获取 Parquet 高性能时序数据文件")
        
    # 绑定按钮事件
    btn.click(
        fn=start_scraping, 
        outputs=[data_table, download_btn, status_text]
    )

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)