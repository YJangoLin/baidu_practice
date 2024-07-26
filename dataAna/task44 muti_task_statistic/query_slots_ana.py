"""入口函数:分析统计多任务信息"""

import json
import pandas as pd
from dags.temp.base_temp_es_client_holder import file_list


def extract_query_intent(dataana):
    dataanaJson = json.loads(dataana)
    intents = []
    extension = dataanaJson.get("payload", {}).get("extension", {})
    if isinstance(extension, str):
        extension = json.loads(extension)
    results = extension.get("origin",{}).get("results",[])
    for res in results:
        intents.append(res.get("intent", None))
    query = dataanaJson.get("query", None)
    return query, intents

def analysis(resultData):
    resultData["query"], resultData["intent"] =  zip(*resultData["dataana"].apply(extract_query_intent))
    resultData[["query", "intent"]].to_csv("query_and_intent_data.csv", index=False)

def parse_bos_data(bosDir):
    files = sorted(file_list(bosDir, filetype=".csv.tar"))
    dataFilterList = []
    columnsList = ['seg', 'dataana']
    for index, file in enumerate(files):
        # 分块进行读取
        dataDfList = pd.read_csv(file, compression='zip', chunksize=10000, usecols=columnsList)
        for dataDf in dataDfList:
            notNaDf = dataDf[dataDf["dataana"].notna()]
            if (len(notNaDf) == 0): continue
            f1Df = notNaDf[(notNaDf["dataana"].str.contains('sequence')) & (notNaDf['seg'] == "IOT2ASR")]
            f2Df = f1Df[f1Df["dataana"].str.contains("轻柔|标准擦|强力擦|标准|强力|Max")]
            dataFilterList += f2Df.values.tolist()
    resultData = pd.DataFrame(dataFilterList, columns=columnsList)
    return resultData


if __name__ == "__main__":
    bosDir = "data/task44"
    # 另一个代码库中拉取bos上拉取最近30天的数据
    # 统计分析
    resultDf = parse_bos_data(bosDir)
    analysis(resultDf)
