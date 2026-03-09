import os
import gradio as gr
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from scripts.fetch_worker import run_stock_pipeline
from scripts.fetch_sector import run_sector_pipeline
from scripts.merge_and_push import run_merge_and_push

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def full_workflow(year=0):
    try:
        logging.info(f"=== Starting Data Pipeline (Year: {'YTD' if year==0 else year}) ===")
        
        logging.info("Step 1: Fetching Stock Data...")
        run_stock_pipeline(year=year)
        
        logging.info("Step 2: Fetching Sector Data...")
        run_sector_pipeline()
        
        logging.info("Step 3: Merging, QC, and Pushing to ModelScope...")
        run_merge_and_push(year=year)
        
        logging.info("=== Pipeline Completed Successfully ===")
        return "SUCCESS: Pipeline executed successfully!"
    except Exception as e:
        logging.error(f"Pipeline Failed: {str(e)}")
        return f"FAILED: {str(e)}"

# 定时任务：每天凌晨 1:00 执行当年数据更新 (YTD)
scheduler = BackgroundScheduler()
scheduler.add_job(full_workflow, 'cron', hour=1, minute=0, args=[0])
scheduler.start()

# Gradio Web 界面
with gr.Blocks() as demo:
    gr.Markdown("# 股票量化数据采集系统 (ModelScope 创空间版)")
    
    with gr.Row():
        btn_daily = gr.Button("运行每日更新 (YTD)", variant="primary")
        btn_full = gr.Button("运行全量历史下载 (2005至今)", variant="stop")
        
    status_output = gr.Textbox(label="运行状态日志", lines=5)
    
    btn_daily.click(lambda: full_workflow(year=0), outputs=status_output)
    btn_full.click(lambda: full_workflow(year=9999), outputs=status_output)

if __name__ == "__main__":
    # 创建必要的临时目录
    os.makedirs("temp_parts", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    demo.launch(server_name="0.0.0.0", server_port=7860)
