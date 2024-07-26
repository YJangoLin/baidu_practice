"""入口函数:统计服务类型为媒体服务的MusicBot的各设备请求数量DAU"""

import re
import pandas as pd
import numpy as np
from dags.temp.base_temp_es_client_holder import ESClientHolder, file_list
import json
import math


def dump_version2(sdate, edate, savePath, days=1):
    """
    拉取数据
    Args:
        date (_type_): 拉取数据的日期
    """
    es_client = ESClientHolder(sdate=sdate, edate=edate, savePath=savePath)
    es_client.dump_batch()


def getReData(msg, pattern=r'"logId":"(.*?)",'):
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
        with open(file, "r") as f:
            while True:
                line = f.readline()
                if line:
                    count += 1
                    js = json.loads(line)
                    msg = js["_source"]["message"]
                    #  提取ak, fc, pk
                    ak = getReData(msg, pattern=r'"ak":"(.*?)"')
                    fc = getReData(msg, pattern=r'"fc":"(.*?)"')
                    pk = getReData(msg, pattern=r'"pk":"(.*?)"')
                    if ak is not None and fc is not None and pk is not None:
                        reqIds.append([ak, fc, pk])
                    else:
                        continue
                else:
                    break
    print(f"数据量:{count}")
    df = pd.DataFrame(reqIds, columns=["ak", "fc", "pk"])
    countDf = df.value_counts(subset=["ak", "fc", "pk"])
    countDf.to_excel("bot_id_qps_rs.xlsx")


# 计算指标
if __name__ == "__main__":
    sdate = "2024-06-25 00:00:00"
    edate = "2024-06-26 00:00:00"
    savePath = "data/task39"
    dump_version2(sdate=sdate, edate=edate, savePath=savePath)
    # compute_query_info(data_dir=savePath)
