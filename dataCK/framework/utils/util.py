import os
import re
import pandas as pd
import numpy as np
import json
from base.version2_dump import ESClientHolder
from conf.config import PATTEN, STRCONTRAINCHECK

"""fileDir文件夹下获取fileType类型的文件"""
def get_file(fileDir, fileType):
    filePath = []
    fileList = os.listdir(fileDir)
    for file in  fileList:
        if file.endswith(fileType):
            filePath.append(f"{fileDir}/{fileDir}/{file}")
    return filePath


"""解析获取的json行"""
def jsonParse(msg:str, patten: dict, strContrainCheck, ts):
    resData = {}
    resData.setdefault("ts", ts)
    strContrainReqId = {s: False for s in strContrainCheck.keys()}
    for pattn in patten.keys():
        infoStr = msg
        if pattn == "domain":
            marth = re.search(r'"nluInfo":"(.*?)}"', msg)
            if marth:
                infoStr = marth.group(1)
        mth = re.search(patten.get(pattn), infoStr)
        if mth:
            value = mth.group(1)
        else:
            value = ""
    for strContrain in strContrainCheck.keys():
        if strContrainCheck.get(strContrain):
            if strContrain in msg:
                strContrainReqId[strContrain] = True
        else:
            if strContrain not in msg:
                strContrainReqId[strContrain] = True
        resData.setdefault(patten, value)
    return resData, strContrainReqId


def json_to_csv(jsonPaths, deal=False):
    """_summary_

    Args:
        jsonPaths (_type_): json文件地址
        deal (bool, optional): 是否对df进行处理. Defaults to False.
    """
    dfList = []
    checkList = []
    for jsonPath in jsonPaths:
        strContrainReqIds = {s: [] for s in STRCONTRAINCHECK.keys()}
        resDict = {s: [] for s in PATTEN.keys() }
        with open(jsonPath) as f:
            line = f.readline()
            while line is not None:
                msg = json.loads(line)["_source"]["message"]
                tss = msg.split(" ", 2)
                ts = tss[0] + " " + tss[1]
                resData, strContrainBoolDict = jsonParse(msg, PATTEN, STRCONTRAINCHECK, ts)
                for res in resData.keys(): resDict.get(res).append(resData.get(res))
                for k in strContrainBoolDict.keys(): 
                    if strContrainBoolDict.get(k): 
                        strContrainReqIds.get(k).append(resData["reqId"])
        df = pd.DataFrame(resData)
        strCheckDf = pd.DataFrame(strContrainReqIds)
        dfList.append(df)
        checkList.append(strCheckDf)
        if deal:
            for key in STRCONTRAINCHECK.keys():
                reqIds = strCheckDf[key].drop_duplicates()
                if STRCONTRAINCHECK.get(key):
                    df = df[df["reqId"].isin(reqIds)]
                else:
                    df = df[~df["reqId"].isin(reqIds)]
            df.to_csv(jsonPath.replace(".json", ".csv"), index=False)
        else:
            return dfList, strCheckDf
                


# 来取数据， json格式存储
def download(sdate, endDate, savePath):
    """
    下载数据
    Args:
        sdate (_type_): %Y-%m-%d
        endDate (_type_): %Y-%m-%d
        savePath (_type_): _description_
    """
    es_client = ESClientHolder(sdate=sdate, edate=endDate, savePath=savePath)
    es_client.dump_batch()

# 将所有的csv读取成df并concat
def concatCSV(filePath):
    dataList = []
    for file in filePath:
        df = pd.read_csv(file)
        df.dropna(axis=0, subset=["reqId", "ts"], how='any', inplace=True)
        df.dropna(axis=0, subset=["seg", "type"], how='all', inplace=True)
        df = df[df['jq'].isin(["BD", "SZ", "GZ"])]
        dataList.append(df)
        logId = df[df["logId"].notna()]["logId"].drop_duplicates(keep="first").values
        np.save(file.replace(".csv", ".npy"), logId)
    datadf = pd.concat(dataList, axis=0, ignore_index=True)
    datadf.drop_duplicates(keep="first", inplace=True)


"""计算分位数等信息"""
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