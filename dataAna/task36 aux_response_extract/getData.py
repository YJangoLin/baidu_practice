"""根据具体时间统计elapse_time的方差均值分位数情况"""

import re
import pandas as pd
import numpy as np
import json






def dump_version2(sdate, edate, savePath, days=1):
    from utilsByresId import ESClientHolder
    """
    拉取数据
    Args:
        date (_type_): 拉取数据的日期
    """
    es_client = ESClientHolder(sdate=sdate,edate=edate, savePath=savePath)
    es_client.dump_batch()


def dump_version2_in_shc_model(sdate, edate, savePath, days=1):
    from utilsInShcModel import ESClientHolder
    """
    拉取数据
    Args:
        date (_type_): 拉取数据的日期
    """
    es_client = ESClientHolder(sdate=sdate,edate=edate, savePath=savePath)
    es_client.dump_batch()

def getLogId(msg, pattern = r'"logId":"(.*?)",'):
    logIdMth = re.search(pattern, msg)
    if logIdMth:
        logId = logIdMth.group(1)
        return logId
    else:
        return None



def filter_data(logIdFile, dataFile, save_path):
    logIdList = []
    with open(logIdFile, 'r') as f:
        while True:
            line = f.readline()
            if line:
                js = json.loads(line)
                msg = js["_source"]["message"]
                logId = getLogId(msg)
                if logId is not None:
                    logIdList.append(logId)
            else:
                break
    wf = open(f"{save_path}/data.json", 'w')
    with open(dataFile, 'r') as f:
        while True:
            line = f.readline()
            if line:
                js = json.loads(line)
                msg = js["_source"]["message"]
                logIdMth = getLogId(msg, pattern=r"log_id=(.*?),")
                if logIdMth is None:  
                    logIdMth = getLogId(msg, pattern=r"log_id:(.*?),")
                if logIdMth and logId in logIdList:
                    wf.write(bytes(line, 'utf-8').decode('unicode_escape'))
            else:
                break
    wf.close()
            
def extractQuery(dataFile, save_path):
    wf = open(f"{save_path}/querydata.tsv", 'w')
    with open(dataFile, 'r') as f:
        while True:
            line = f.readline()
            if line:
                js = json.loads(line)
                msg = js["_source"]["message"]
                query = getLogId(msg, pattern=r"query=(.*?);")
                if query:
                    wf.write(f"{query}\n")
            else:
                break
    wf.close()
    querydata = pd.read_csv(f"{save_path}/querydata.tsv", sep="\t", header=None)
    querydata.columns = ["query"]
    countDf = querydata.value_counts()
    sortDf = countDf.sort_values(ascending=False)
    sortDf.to_csv(f"{savePath}/querydata(dup).tsv", sep="\t")

if __name__ == "__main__":
    sdate = "2024-06-12 00:00:00"
    edate = "2024-06-17 15:30:00"
    logIdFile = "data/task36/es_origin_data_00:00:00.json"
    dataFile = "data/task36/shcModel/es_origin_data_00:00:00.json"
    savePath = "data/task36/shcModel"
    # 30qps_not_sleep 40qps_not_sleep 50qps_not_sleep 60qps_not_sleep
    # dump_version2_in_shc_model(sdate=sdate, edate=edate, savePath=savePath)
    extractQuery("data/task36/shcModel/data.json", savePath)
    # filter_data(logIdFile=logIdFile, dataFile=dataFile, save_path=savePath)
    # compute_indicator(savePath, 60)