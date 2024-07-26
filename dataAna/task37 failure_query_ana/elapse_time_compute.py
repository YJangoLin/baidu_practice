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


def compute_indicator(data_dir):
    """计算方差、均值、分位数等指标

    Args:
        data_dir (_type_): json文件保存的文件夹

    Returns:
        _type_: list
    """
    files = sorted(file_list(data_dir, filetype=".json"))
    my_list = []
    count = 0
    # fw = open(f"{data_dir}/data1.json", "w")
    for index, file in enumerate(files):
        with open(file, 'r') as f:
            while True:
                line = f.readline()
                if line:
                    js = json.loads(line)
                    msg = js["_source"]["message"]
                    #  提取ts
                    # domain = getReData(msg, pattern=r'\\"domain\\":\\"(.*?)\\"')
                    query = getReData(msg, pattern=r'"query":"(.*?)"')
                    # intent = getReData(msg, pattern=r'\\"intent\\":\\"(.*?)\\"')
                    speechId = getReData(msg, pattern=r'"speechId":(.*?),')
                    ts = msg.split("  ")[0]
                    if speechId is not None and query is not None and ts is not None:
                        # fw.write(line)
                        my_list.append([query, speechId, ts])
                        count +=1
                    else:
                        continue
                else:
                    break
    # fw.close()
    df = pd.DataFrame(my_list, columns=["query", "speechId", "datetime"])
    # sc = df.value_counts()
    # sc.columns = ["count"]
    df.drop_duplicates(inplace=True)
    df.to_excel("data_query_返回基站.xlsx", index=False)



def compute_query_info(sdate, edate, savePath, data_dir):
    """计算方差、均值、分位数等指标

    Args:
        data_dir (_type_): json文件保存的文件夹

    Returns:
        _type_: list
    """
    files = [data_dir]
    reqIds = []
    for index, file in enumerate(files):
        with open(file, 'r') as f:
            while True:
                line = f.readline()
                if line:
                    js = json.loads(line)
                    msg = js["_source"]["message"]
                    #  提取ts
                    reqId = getReData(msg, pattern=r'  : \[(.*?)\]')
                    # ts = msg.split("  ")[0]
                    if reqId is not None:
                        reqIds.append(reqId)
                    else:
                        continue
                else:
                    break
    reqIds = list(set(reqIds))
    es_client = ESClientHolder(sdate=sdate,edate=edate, savePath=savePath)
    es_client.dump_batch(reqIds)

# 计算指标
if __name__ == "__main__":
    sdate = "2024-06-12 00:00:00"
    edate = "2024-06-20 00:00:00"
    savePath = "data/task37/reqId"
    dump_version2(sdate=sdate, edate=edate, savePath=savePath)
    compute_indicator(savePath)
    # compute_query_info(sdate=sdate, edate=edate, savePath=savePath, data_dir="data/task37/data1.json")
