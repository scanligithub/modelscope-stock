import os
import gradio as gr
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from pytz import timezone

# 导入你的核心业务逻辑
from scripts.fetch_worker import run_stock_pipeline
from scripts.fetch_sector import run_sector_pipeline
from scripts.merge_and_push import run_merge_and_push

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =======================================================
# 1. 核心业务逻辑 (供调度器和前端共用)
# =======================================================
def execute_pipeline(year):
    run_stock_pipeline(year=year)
    run_sector_pipeline()
    run_merge_and_push(year=year)

# =======================================================
# 2. 供后台定时任务使用的函数 (阻塞式)
# =======================================================
def cron_workflow(year=0):
    try:
        logging.info(f"=== [Cron] Starting Data Pipeline (Year: {'YTD' if year==0 else year}) ===")
        execute_pipeline(year)
        logging.info("=== [Cron] Pipeline Completed Successfully ===")
    except Exception as e:
        logging.error(f"[Cron] Pipeline Failed: {str(e)}")

# =======================================================
# 3. 供 Gradio 前端使用的函数 (流式生成器 yield)
# =======================================================
def ui_workflow(year=0):
    mode = "YTD (今年初至今)" if year == 0 else "全量历史 (2005至今)"
    log_text = f"🚀 手动任务已启动：模式 [{mode}]\n"
    yield log_text
    
    try:
        log_text += "\n⏳ [1/3] 正在获取 A 股数据，请耐心等待 (约需几分钟)...\n💡 提示：详细的下载进度条请查看创空间的【运行日志】控制台面板。\n"
        yield log_text
        run_stock_pipeline(year=year)
        
        log_text += "✅ A 股数据获取完成！\n\n⏳ [2/3] 正在拉取东财板块行情与成分股...\n"
        yield log_text
        run_sector_pipeline()
        
        log_text += "✅ 板块数据拉取完成！\n\n⏳ [3/3] 正在使用 DuckDB 合并数据并推送到 ModelScope 数据集...\n"
        yield log_text
        run_merge_and_push(year=year)
        
        log_text += "\n🎉 恭喜！所有流水线任务全部成功完成！"
        yield log_text
        
    except Exception as e:
        log_text += f"\n❌ 任务发生严重错误: {str(e)}"
        yield log_text


# =======================================================
# 4. 初始化系统与定时任务
# =======================================================
# 设定时区为北京时间，防止 Docker 容器默认 UTC 导致定时器错乱
scheduler = BackgroundScheduler(timezone=timezone('Asia/Shanghai'))
scheduler.add_job(cron_workflow, 'cron', hour=1, minute=0, args=[0])
scheduler.start()

# =======================================================
# 5. 构建 Gradio Web UI
# =======================================================
with gr.Blocks(theme=gr.themes.Soft()) as demo:
    gr.Markdown("# 📈 股票量化数据采集系统 (ModelScope 创空间版)")
    gr.Markdown("本项目由后台容器托管，支持自动定时更新与手动全量拉取。")
    
    with gr.Row():
        btn_daily = gr.Button("▶️ 运行每日更新 (YTD)", variant="primary")
        btn_full = gr.Button("⚠️ 运行全量历史下载 (2005至今)", variant="stop")
        
    status_output = gr.Textbox(
        label="运行状态播报", 
        lines=12, 
        placeholder="点击上方按钮开始执行任务，日志将在此处流式输出..."
    )
    
    # 绑定事件：调用带有 yield 的 ui_workflow
    btn_daily.click(fn=lambda: ui_workflow(year=0), outputs=status_output)
    btn_full.click(fn=lambda: ui_workflow(year=9999), outputs=status_output)

if __name__ == "__main__":
    # 创建必要的临时目录，防止文件找不到报错
    os.makedirs("temp_parts", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    
    # 启动 Web 服务
    demo.launch(server_name="0.0.0.0", server_port=7860)
    