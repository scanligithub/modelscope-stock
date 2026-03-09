import gradio as gr
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from scripts.fetch_worker import run_stock_pipeline
from scripts.fetch_sector import run_sector_pipeline
from scripts.merge_and_push import run_merge_and_push
import os

logging.basicConfig(level=logging.INFO)

def full_workflow():
    try:
        logging.info("Starting Daily Workflow...")
        run_stock_pipeline()
        run_sector_pipeline()
        run_merge_and_push()
        return "SUCCESS: Workflow finished."
    except Exception as e:
        return f"FAILED: {str(e)}"

# 定时任务：每天凌晨 1 点执行
scheduler = BackgroundScheduler()
scheduler.add_job(full_workflow, 'cron', hour=1, minute=0)
scheduler.start()

with gr.Blocks() as demo:
    gr.Markdown("# 量化数据采集系统 (ModelScope)")
    status = gr.Textbox(label="运行状态", value="Idle")
    btn = gr.Button("手动触发采集")
    btn.click(full_workflow, outputs=status)

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)
