"""入口函数:统计服务类型为媒体服务的MusicBot的各设备请求数量DAU"""

import os
import re
import glob
import pandas as pd
import numpy as np
from dags.temp.base_temp_es_client_holder import ESClientHolder, file_list
import json
import math
from datetime import datetime, timedelta
from dags.config.logging import logging
from dags.config.constant import root_dir_v2


def dump_version2(sdate, edate, savePath, days=1):
    """
    拉取数据
    Args:
        date (_type_): 拉取数据的日期
    """
    es_client = ESClientHolder(sdate=sdate,edate=edate, savePath=savePath)
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

def getCachereqId(data_dir, isCache=False):
    files = sorted(file_list(data_dir, filetype=".json"))
    count = 0
    reqIds = []
    for index, file in enumerate(files):
        result = []
        with open(file, 'r') as f:
            while True:
                line = f.readline()
                if line:
                    count += 1
                    js = json.loads(line)
                    msg = js["_source"]["message"]
                    #  提取ts"bot_id":"rock_sweep"
                    req_id = getReData(msg, pattern=r': \[(.*?)\]')
                    if isCache: 
                        if req_id is not None and "\u7f13\u5b58\u5339\u914d" in msg:
                            result.append(req_id)
                    else:
                        if req_id is not None:
                            result.append(req_id)
                else:
                    break
        reqIds.append(list(set(result)))
        print(f"{file}: {str(len(result))}")
    print(f"dataNumber:{str(count)}")
    return reqIds


def merge(cache_data_dir, unit_data_dir):
    "receive unit service response", ""
    """
    拉取数据
    Args:
        date (_type_): 拉取数据的日期
    """
    # logIds = getLogId(dataDir)
    cacheIds = getCachereqId(cache_data_dir, isCache=True)
    unitIds = getCachereqId(unit_data_dir, isCache=False)
    # cacheId, unitId = [], []
    # for c in cacheIds: cacheId += c
    # for u in unitIds: unitId += u
    files = sorted(file_list(cache_data_dir, filetype=".json"))
    count= 0
    queryF = open("./7days_queryAndResponse.txt", "w")
    for index, file in enumerate(files):
        cacheId = cacheIds[index]
        unitId = unitIds[index]
        with open(file, 'r') as f:
            while True:
                line = f.readline()
                if line:
                    count+=1
                    js = json.loads(line)
                    msg = js["_source"]["message"]
                    req_id = getReData(msg, pattern=r': \[(.*?)\]')
                    cache = 0
                    if "\u7f13\u5b58\u5339\u914d" in msg or req_id is None:
                        continue
                    if req_id not in unitId: continue
                    if req_id in cacheId:  cache = 1         
                    #  提取ts"bot_id":"rock_sweep"
                    # 提取 query 和 response
                    query = getReData(msg, 'query=(.*?),')
                    startIndex = msg.find("response =")
                    if startIndex != -1:
                        reponse = msg[startIndex + 10 : -1].strip()
                        queryF.write("{}\t{}\t{}\t{}\n".format(req_id, query, reponse, str(cache)))
                else:
                    break
    print(f"count:{count}")
    queryF.close()
    queryAndResponseDf = pd.read_csv("./7days_queryAndResponse.txt", sep="\t", header=None)
    queryAndResponseDf.columns = ["req_id", "query", "response", "cache"]
    req_id = queryAndResponseDf.loc[queryAndResponseDf["cache"]==1, "req_id"]
    # 将6.29 ～ 30号的query的cache设置为1
    pastQueryDf = pd.read_excel("dags/temp/task41 query_response_logId/query_count_subpast_29_30_1.xlsx")
    pastQuery = pastQueryDf.loc[pastQueryDf["count"] >= 2, "query"]
    queryAndResponseDf.loc[queryAndResponseDf["query"].isin(pastQuery), "cache"] = 1
    
    # 将一次缓存命中query中全部标记为缓存命中
    cacheQuery = queryAndResponseDf.loc[queryAndResponseDf["cache"]==1, "query"]
    queryAndResponseDf.loc[queryAndResponseDf["query"].isin(cacheQuery), "cache"] = 1
    
    queryAndResponseDf.loc[queryAndResponseDf["req_id"].isin(req_id), "cache"] = 1
    queryAndResponseDf.drop_duplicates(inplace=True)
    ratedio = queryAndResponseDf[queryAndResponseDf["cache"]==0]["req_id"].nunique() / queryAndResponseDf["req_id"].nunique()
    print(f"排除前天加入缓存数据未中缓存的query占比：{str(round(ratedio, 3))}")
    noCacheDf = queryAndResponseDf.loc[queryAndResponseDf["cache"]==0, ["query", "response"]]
    # 需要把6.29～30号的query = 1 的和当前的合并统计
    pastCaQuery = pastQueryDf.loc[pastQueryDf["count"] == 1, ["query", "response"]]
    noCacheDf = pd.concat([pastCaQuery, noCacheDf], axis=0, ignore_index=True)
    
    countDf = noCacheDf.groupby(by="query").count()
    dupDf = noCacheDf.drop_duplicates(subset=["query"], keep="first")
    countDf.reset_index(inplace=True)
    mergeDf = pd.merge(dupDf,countDf, how="inner", on="query")
    mergeDf.columns = ["query", "response", "count"]
    mergeDf.sort_values(by="count", ascending=False).to_excel("query_count.xlsx", index=False)
    dupNum = mergeDf.loc[mergeDf["count"] >= 2, "count"].sum()
    allTotal = mergeDf["count"].sum()
    print(f"重复数据的占比为：{round(dupNum/allTotal, 3)*100}%")


# 计算指标
if __name__ == "__main__":
    sdate = "2024-07-02 00:00:00"
    edate = "2024-07-03 00:00:00"
    savePath = "data/task41/cacheResponse"
    # savePath = f"data/version2_0/{sdate}/analysis/origin-my_index-{sdate}.csv.zip"
    '''
    shc_v1*
    es1
    "must": [
            {"match_phrase": {"message": "UnitOrigin Entire Response"}},
            {"match_phrase": {"message": "1104379"}}
        ]
    es2
    "should": [
            {"match_phrase": {"message": "receive unit service response"}},
            {"match_phrase": {"message": "缓存匹配"}}
            ],
        "minimum_should_match" : 1
    '''
    # dump_version2(sdate=sdate, edate=edate, savePath=savePath)
    # 391,101 -- 922,535
    merge("data/task41/cacheResponse", "data/task41")
