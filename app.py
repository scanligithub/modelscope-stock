import os
import logging
import functools
from datetime import datetime
from pytz import timezone

import gradio as gr
from apscheduler.schedulers.background import BackgroundScheduler

# 导入核心业务逻辑脚本
from scripts.fetch_worker import run_stock_pipeline
from scripts.fetch_sector import run_sector_pipeline
from scripts.merge_and_push import run_merge_and_push

# 配置日志格式
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# =======================================================
# 1. 核心业务流程 (供内部调用)
# =======================================================
def execute_pipeline(year):
    """
    顺序执行抓取、合并、上报流程
    """
    run_stock_pipeline(year=year)
    run_sector_pipeline()
    run_merge_and_push(year=year)

# =======================================================
# 2. 后台定时任务函数 (Cron Job)
# =======================================================
def cron_workflow(year=0):
    try:
        logging.info(f"=== [Cron] 定时任务启动 (模式: {'YTD' if year==0 else 'FULL'}) ===")
        execute_pipeline(year)
        logging.info("=== [Cron] 定时任务成功完成 ===")
    except Exception as e:
        logging.error(f"[Cron] 定时任务失败: {str(e)}")

# =======================================================
# 3. 前端交互流式函数 (UI Yield Generator)
# =======================================================
def ui_workflow(year=0):
    """
    使用 yield 实现 Gradio 前端的流式日志输出
    """
    mode_str = "今年初至今 (YTD)" if year == 0 else "全量历史 (2005-至今)"
    log_stream = f"🚀 任务已启动 | 模式: {mode_str}\n"
    log_stream += f"⏰ 启动时间: {datetime.now(timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')}\n"
    yield log_stream
    
    try:
        # 步骤 1
        log_stream += "\n⏳ [1/3] 正在获取 A 股全量 K 线与资金流数据...\n"
        log_stream += "   (此过程需访问 Baostock 和新浪接口，约需 5-15 分钟)\n"
        log_stream += "   💡 详细进度请实时查看魔塔空间的【日志】面板。\n"
        yield log_stream
        run_stock_pipeline(year=year)
        
        # 步骤 2
        log_stream += "✅ A 股行情数据下载完成。\n\n⏳ [2/3] 正在获取东财板块行业/概念行情及成分股...\n"
        yield log_stream
        run_sector_pipeline()
        
        # 步骤 3
        log_stream += "✅ 板块数据获取完成。\n\n⏳ [3/3] 正在执行 DuckDB 数据聚合、质量检查并推送至 ModelScope 数据集...\n"
        yield log_stream
        run_merge_and_push(year=year)
        
        log_stream += "\n🎉 ========================================\n"
        log_stream += "🎉 所有流水线任务已全部成功完成！数据已同步至云端。\n"
        log_stream += "🎉 ========================================"
        yield log_stream
        
    except Exception as e:
        error_msg = f"\n❌ 任务运行出错: {str(e)}"
        logging.error(error_msg)
        log_stream += error_msg
        yield log_stream

# =======================================================
# 4. 初始化定时调度器 (北京时间)
# =======================================================
beijing_tz = timezone('Asia/Shanghai')
scheduler = BackgroundScheduler(timezone=beijing_tz)

# 设定每天凌晨 01:00 执行每日增量更新
scheduler.add_job(cron_workflow, 'cron', hour=1, minute=0, args=[0])
scheduler.start()
logging.info("⏰ 后台调度器已启动，北京时间凌晨 01:00 自动执行更新。")

# =======================================================
# 5. 构建 Gradio Web 界面
# =======================================================
with gr.Blocks() as demo:
    gr.Markdown("# 📊 股票量化数据采集系统 (ModelScope 创空间)")
    gr.Markdown(
        "本项目基于 **Python + Polars + DuckDB** 构建，支持全自动定时采集与手动触发。"
        "\n数据将自动持久化至关联的 ModelScope Dataset 仓库。"
    )
    
    with gr.Row():
        btn_daily = gr.Button("▶️ 运行每日更新 (YTD)", variant="primary")
        btn_full = gr.Button("⚠️ 运行全量历史下载", variant="secondary")
        
    status_output = gr.Textbox(
        label="任务实时播报 (Live Logs)", 
        lines=15, 
        placeholder="等待指令中... 点击按钮后此处将流式播报进度。"
    )
    
    # 绑定点击事件
    btn_daily.click(
        fn=functools.partial(ui_workflow, year=0), 
        inputs=None, 
        outputs=status_output
    )
    
    btn_full.click(
        fn=functools.partial(ui_workflow, year=9999), 
        inputs=None, 
        outputs=status_output
    )

if __name__ == "__main__":
    # 确保运行目录存在
    os.makedirs("temp_parts", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    
    # 启动应用
    # 开启 queue() 以支持 yield 流式输出
    demo.queue().launch(
        server_name="0.0.0.0", 
        server_port=7860, 
        theme=gr.themes.Soft()
    )