"""根据具体时间统计elapse_time的方差均值分位数情况"""

import re
import pandas as pd
import numpy as np
from utilsByresId import ESClientHolder, file_list
import json
import math





def dump_version2(sdate, edate, savePath, days=1):
    """
    拉取数据
    Args:
        date (_type_): 拉取数据的日期
    """
    es_client = ESClientHolder(sdate=sdate,edate=edate, savePath=savePath)
    es_client.dump_batch()

def getReData(msg, pattern = r'"logId":"(.*?)",'):
    """匹配message字段中的logId

    Args:
        msg (_type_): message字符串
        pattern (regexp, optional): 匹配的正则函数. Defaults to r'"logId":"(.*?)",'.

    Returns:
        _type_: 提取的logId
    """
    logIdMth = re.search(pattern, msg)
    if logIdMth:
        logId = logIdMth.group(1)
        return logId
    else:
        return None


def compute_q(serise):
    mean = round(serise.mean(), 1)
    std = round(serise.std(), 1)
    q_5 = round(serise.quantile(0.5, interpolation="lower"), 1)
    q_9 = round(serise.quantile(0.9, interpolation="lower"), 1)
    q_99 = round(serise.quantile(0.99, interpolation="lower"), 1)
    q_999 = round(serise.quantile(0.999, interpolation="lower"), 1)
    q_9999 = round(serise.quantile(0.9999, interpolation="lower"), 1)
    print(
        f"std: {std} \t mean: {mean}\t q_50: {q_5} \t q_90: {q_9} \t q_99: {q_99} \t q_999: {q_999}\t q_9999: {q_9999}"
    )
    return (std, mean, q_5, q_9, q_99, q_999, q_9999)



def compute_query_info(data_dir):
    """计算方差、均值、分位数等指标

    Args:
        data_dir (_type_): json文件保存的文件夹

    Returns:
        _type_: list
    """
    files = sorted(file_list(data_dir, filetype=".json"))
    count = 0
    reqIds = []
    for index, file in enumerate(files):
        with open(file, 'r') as f:
            while True:
                line = f.readline()
                if line:
                    count += 1
                    js = json.loads(line)
                    msg = js["_source"]["message"]
                    #  提取ts"bot_id":"rock_sweep"
                    bot_id = getReData(msg, pattern=r'"bot_id":"(.*?)"')
                    ts = msg.split("  ")[0]
                    if bot_id is not None:
                        reqIds.append([bot_id, ts])
                    else:
                        continue
                else:
                    break
    print(f"数据量:{count}")
    df = pd.DataFrame(reqIds, columns=["bot_id", "ts"])
    df["sec"] = df["ts"].apply(lambda x: x[:-4])
    countDf = df.groupby(["bot_id", "sec"]).count()
    countDf.to_excel("bot_id_qps.xlsx")
    countDf.reset_index(inplace=True)
    countDf.columns = ["bot_id", "date", "qps"]
    countDf.to_excel("bot_id_qps_rs.xlsx", index=False)
    bot_id_index = countDf["bot_id"].value_counts().index
    resList = []
    for bot_id in bot_id_index:
        print(bot_id)
        q_indent = compute_q(countDf[countDf["bot_id"]==bot_id]["qps"])
        resList.append([bot_id]+ list(q_indent))
    # 绘制一个可视化图
    pd.DataFrame(resList, columns=["bot_id", 'std', 'mean', 'q_50', 'q_90', 'q_99', 'q_999', 'q_9999']).to_excel("indent.xlsx", index=False)
def plot_line(xlsxPath):
    pass
    
# 计算指标
if __name__ == "__main__":
    sdate = "2024-06-19 15:00:00"
    edate = "2024-06-20 15:00:00"
    savePath = "data/task38"
    # dump_version2(sdate=sdate, edate=edate, savePath=savePath)
    # compute_indicator(savePath)
    compute_query_info(data_dir=savePath)
