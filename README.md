# 股票量化数据采集系统 (ModelScope 创空间版)

本项目是基于阿里魔塔 (ModelScope) 创空间的 A 股全量历史与实时行情采集系统。

### 特性
1. **常驻调度**：通过 `APScheduler` 实现每日凌晨自动执行采集任务。
2. **多进程并发**：基于 `ProcessPoolExecutor` 实现安全的单机高并发抓取。
3. **抗风控防封**：内建 Cloudflare Worker 代理穿透与连接池复用技术。
4. **自动入库**：基于 DuckDB 聚合 Parquet 数据并自动推送到 ModelScope Dataset。

### 环境变量配置说明
在魔塔创空间“设置 -> 环境变量”中配置以下三个变量：
- `CF_WORKER_URL`: 你的 Cloudflare Worker 代理地址 (如 `xxx.workers.dev`)
- `MS_TOKEN`: 你的 ModelScope Access Token
- `MS_DATASET_ID`: 目标数据集仓库 (如 `yourname/ashare-data`)
