"""入口函数:分析slot关键字"""

import json
import yaml
import os
import re
import pandas as pd




def parser_yml(ymlDir):
    """解析yml文件中需要匹配的intent

    Args:
        ymlDir (_type_): yml文件夹

    Returns:
        _type_: _description_
    """
    ymlFilePaths = [ymlDir + "/" + ymlName for ymlName in os.listdir(ymlDir)]
    intentData = {}
    for ymlPath in ymlFilePaths:
        with open(ymlPath, 'r') as file:
            data = yaml.safe_load(file)
            topKey = list(data.keys())[0]
            intentData.setdefault(topKey, list(data.get(topKey).keys()))
    return intentData

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

def slot_swich(slots, query):
    """提取slot中关键字段

    Args:
        slots (_type_): slots json字符串
        query (_type_): 对应的query

    Returns:
        _type_: _description_
    """
    slotsList = []
    for slot_type in slots.keys():
        slottypeList = slots.get(slot_type, [])
        for slottype in slottypeList:
            text = slottype.get("text")
            # value = str(slottype.get("value"))
            if (slot_type == 'frequency'):
                startIndex = query.find(text)
                slotsList.append(['frequency', startIndex, startIndex+len(text)])
                continue
            if "和 " in text:
                # value = value.replace("he ", "")
                text = text.replace("和 ", "")
            text = text.split(" ")
            for i in range(len(text)):
                t = text[i]
                startIndex = query.find(t)
                if startIndex == -1:
                    print(f"query:{query}  slot:{text} ")
                    return None
                slotsList.append([slot_type, startIndex, startIndex+len(t)])
    return slotsList

def slots_deal(row):
    """slots规格化的处理

    Args:
        row (_type_): 记录

    Returns:
        _type_: _description_
    """
    query = row["query"]
    x = row["asr2iot"]
    datas = json.loads(x)[0]
    domain, intent, slots = datas[0], datas[1], datas[2]
    slotsList = []
    if slots == {}: slotsList = None
    else: 
        slotsList = slot_swich(slots=slots, query=query)
        if slotsList is None:
            slotsList = "error"
    return [query, domain, intent, slotsList]

def parse_bos_data(filePath):
    """解析bos中拉取的压缩文件，并进行一定的规格化处理

    Args:
        filePath (_type_): 文件地址

    Returns:
        _type_: 解析结果
    """
    dataDf = pd.read_csv(filePath,sep='\t')
    dataList = dataDf[["query", "asr2iot"]].apply(slots_deal, axis=1)
    resultDf = pd.DataFrame(dataList.values.tolist(), columns=["query", "domain", "intent", "slots"])
    return resultDf

def analysis(resultDf, intents):
    """聚合过滤生成最终文件

    Args:
        resultDf (_type_): pandas 解析的bos数据
        intents (_type_): 需要匹配的山川的intents
    """
    resultDf[resultDf["intent"].isin(intents)].drop_duplicates(["query"], keep="first").to_csv("intent_static_data.tsv", index=False, sep="\t")
    intentDataNum = []
    for intent in intents:
        dataNum = len(resultDf[resultDf["intent"] == intent])
        intentDataNum.append([intent, dataNum])
    intentDataDf = pd.DataFrame(intentDataNum, columns=["intent", "count"])
    intentDataDf.sort_values("count", ascending=False).to_csv("intent_data_num.tsv", index=False, sep="\t")

if __name__ == "__main__":
    # 数据拉取部分是从doccano代码库中拉取
    dataInput = "dags/temp/task43/inputData"
    savePath = "data/task43"
    resultDf = parse_bos_data(savePath + "asr2iot_iot2asr.csv")
    intentDict = parser_yml(dataInput)
    intentList = []
    for intentValues in intentDict.values(): intentList += intentValues
    resultDf = resultDf[resultDf["slots"] != "error"]
    analysis(resultDf, intentList)