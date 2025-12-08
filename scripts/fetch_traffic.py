import requests
import pandas as pd
import os
from datetime import datetime, timedelta
import time

# ParisData API URL
url = "https://parisdata.opendatasoft.com/api/records/1.0/search/"

# 计算目标时间窗口（前两天当前小时）
now = datetime.utcnow()
target_start = now - timedelta(days=2)
target_start = target_start.replace(minute=0, second=0, microsecond=0)
target_end = target_start + timedelta(hours=1)

# 创建 data 文件夹
os.makedirs("data", exist_ok=True)

# 按月份生成 Parquet 文件，例如 data/traffic_2025-12.parquet
output_file = f"data/traffic_{target_start.strftime('%Y-%m')}.parquet"

# 分页参数
all_records = []
start_idx = 0
page_size = 10000

while True:
    params = {
        "dataset": "comptages-routiers-permanents",
        "rows": page_size,
        "start": start_idx,
        "sort": "-t_1h",
        "q": (
            f"t_1h >= '{target_start.isoformat()}' AND "
            f"t_1h < '{target_end.isoformat()}'"
        )
    }

    # 网络请求重试
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            if attempt < 2:
                time.sleep(5)  # 等待 5 秒重试
            else:
                raise Exception(f"Failed to fetch data after 3 attempts: {e}")

    records = [rec["fields"] for rec in data.get("records", [])]
    if not records:
        break

    all_records.extend(records)
    start_idx += page_size
    if len(records) < page_size:
        break

# 保存数据
if all_records:
    df = pd.DataFrame(all_records)
    # 如果文件已存在，则读取旧数据并去重
    if os.path.exists(output_file):
        df_existing = pd.read_parquet(output_file)
        df_all = pd.concat([df_existing, df], ignore_index=True)
        # 根据时间和路段 ID 去重
        if "id_pmr" in df_all.columns:
            df_all.drop_duplicates(subset=["t_1h", "id_pmr"], inplace=True)
        df_all.to_parquet(output_file, index=False)
    else:
        df.to_parquet(output_file, index=False)
