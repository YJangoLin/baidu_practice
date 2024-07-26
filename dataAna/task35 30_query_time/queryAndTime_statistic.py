"""统计aux设备的response和aux_schedule_management信息"""
import os
import re
import pandas as pd
import numpy as np
from utilsByresId import ESClientHolder, file_list
import json
from bos_download_data import dump_es_data





def dump_version2(sdate, edate, savePath, days=1):
    """
    拉取数据
    Args:
        date (_type_): 拉取数据的日期
    """
    es_client = ESClientHolder(sdate=sdate,edate=edate, savePath=savePath)
    es_client.dump_batch()

    

def getTime(data_dir):
    """获取数据中time和query

    Args:
        data_dir (_type_): json文件保存的文件夹

    Returns:
        _type_: list
    """
    files = sorted(file_list(data_dir, filetype=".json"))
    count = 0
    wirteFile = open("data.txt", "w")
    # resList = [] 
    for index, file in enumerate(files):
        with open(file, 'r') as f:
            while True:
                count += 1
                print(f"count:{count}")
                line = f.readline()
                if line:
                    js = json.loads(line)
                    msg = js["_source"]["message"]
                    startIndex = msg.find("{")
                    if startIndex == -1:
                        continue
                    else:
                        msg = msg[startIndex:]
                    msgDict = json.loads(msg)
                    if isinstance(msgDict.get("content", {}), str):
                        continue
                    directives = msgDict.get("content", {}).get("directives", [])
                    for directive in directives:
                        extension = directive.get("payload", {}).get("extension")
                        if extension is not None:
                            extenJson = json.loads(extension)
                            query = extenJson.get('origin', {}).get('query')
                            results = extenJson.get("origin", {}).get('results', [])
                            for result in results:
                                if result is not None:
                                    # print(result)
                                    slots = result.get('slots', {})
                                    timeList = slots.get('time') if slots is not None else None
                                    if query is not None and timeList is not None:
                                        timeStr = ""
                                        for timel in timeList:
                                            timeStr += "\t" + bytes(json.dumps(timel), 'utf-8').decode('unicode_escape')
                                        wirteFile.write("{}{}\n".format(query, timeStr))
                                        # resList.append([query, timeList])
                else:
                    break
    print(f"count: {count}")
    wirteFile.close()


def getQueryAndTime(x, wirteFile):
    contentJs = json.loads(x)
    session = contentJs.get("session")
    if isinstance(session, str):
        sessionJs = json.loads(session)
    elif isinstance(session, dict):
        sessionJs = [session]
    else: return None
    for sess in sessionJs:
        query = sess.get("query")
        if query is None:
            return None
        results = sess.get("nlu", {}).get('results', [])
        for result in results:
            if result is not None:
                # print(result)
                slots = result.get('slots', {})
                timeList = slots.get('time') if slots is not None else None
                if query is not None and timeList is not None:
                    timeStr = ""
                    for timel in timeList:
                        timeStr += "\t" + bytes(json.dumps(timel), 'utf-8').decode('unicode_escape')
                    wirteFile.write("{}{}\n".format(query, timeStr))
    return None


def getBosTime(data_dir, date):
    wirteFile = open(f"data/data_{date}.txt", "w")
    reader = pd.read_csv(data_dir, compression='zip', usecols=["content", "seg"], chunksize=10000)
    for chuck in reader:
        contentDf = chuck
        contentDf = contentDf[(contentDf["seg"] == "IOT2ASR") & (contentDf["content"].str.contains('time_range')) | (contentDf["content"].str.contains('time_point'))]["content"]
        _ = contentDf.apply(lambda x :getQueryAndTime(x, wirteFile))
    wirteFile.close()


def mergeInfo(dataDir):
    fileNames = os.listdir(dataDir)
    filePaths = [f"{dataDir}/{filename}" for filename in  fileNames]
    dataList = []
    for file in filePaths:
        csvDf = pd.read_csv(file, sep='\t', header=None, names=["query", "time_1", "time_2", "time_3"], encoding="utf-8")
        dataList.append(csvDf)
    mergeDf = pd.concat(dataList, axis=0, ignore_index=True)
    dataDf = pd.concat([mergeDf, mergeDf], axis=0, ignore_index=True)
    dataDf.drop_duplicates(inplace=True)
    len(dataDf)

if __name__ == "__main__":
    edate = "2024-06-12"
    sdate = "2024-05-12"
    savePath = "data/task35"
    getTime(savePath)
    dump_es_data(ds="2024-05-12", index=None)
    getBosTime(data_dir="data/version2/analysis/es_origin_data_00:00:00.csv.zip",date=sdate)
    mergeInfo(dataDir="./data")
