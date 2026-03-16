import gradio as gr
import asyncio
import math
import random
import polars as pl
from curl_cffi.requests import AsyncSession

# --- 将我们之前写好的 fetch_with_retry, fetch_single_board_type 核心逻辑复制过来 ---
# (为了节省篇幅，这里用省略号代替，你只需要把上一轮我给你的核心函数原样贴在这里)
SEMAPHORE = asyncio.Semaphore(5)
EASTMONEY_NODES = ["push2.eastmoney.com", "11.push2.eastmoney.com", "82.push2.eastmoney.com"]
BOARD_TYPES = {"行业": "m:90 t:2", "概念": "m:90 t:3", "地域": "m:90 t:1"}

# 这里放置 get_default_params, fetch_with_retry, fetch_single_board_type 函数...
# ... [原样粘贴 V4 版本中的这三个函数] ...

async def core_scraping_logic():
    async with AsyncSession(impersonate="chrome120") as session:
        all_boards_data = []
        for b_name, fs_val in BOARD_TYPES.items():
            board_data = await fetch_single_board_type(session, b_name, fs_val)
            all_boards_data.extend(board_data)
            await asyncio.sleep(1)

        if not all_boards_data:
            return None, "抓取失败，未获取到数据"

        df = pl.DataFrame(all_boards_data).select([
            pl.col("board_type").alias("板块大类"),
            pl.col("f12").alias("板块代码"),
            pl.col("f14").alias("板块名称"),
            (pl.col("f2") / 100).alias("最新点位"),
            pl.col("f3").alias("涨跌幅(%)"),
            (pl.col("f62") / 1e8).alias("主力净流入(亿元)")
        ]).filter(pl.col("板块代码") != "-").sort(["板块大类", "涨跌幅(%)"], descending=[False, True])
        
        # 保存文件供下载
        file_path = "all_boards_merged.parquet"
        df.write_parquet(file_path)
        
        # 转换为 Pandas 供 Gradio 表格展示
        return df.to_pandas(), file_path

# --- Gradio UI 界面 ---
def start_scraping():
    # Gradio 中运行 asyncio 任务
    df_pandas, file_path = asyncio.run(core_scraping_logic())
    if df_pandas is None:
        return None, None, file_path # 返回错误信息
    return df_pandas, file_path, "抓取成功！"

with gr.Blocks(title="东财量化数据中台") as demo:
    gr.Markdown("## 东方财富 - 三大板块全量极速抓取 (国内云端节点)")
    
    with gr.Row():
        btn = gr.Button("🚀 一键全量抓取", variant="primary")
        status_text = gr.Textbox(label="运行状态", interactive=False)
        
    with gr.Row():
        # 展示前50条数据
        data_table = gr.Dataframe(label="数据预览 (支持排序)")
        download_btn = gr.File(label="下载 Parquet 源文件")
        
    btn.click(
        fn=start_scraping, 
        outputs=[data_table, download_btn, status_text]
    )

demo.launch()