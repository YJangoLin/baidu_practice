"""从测试文档html文件中，爬取错误日志中的query并分析"""

from DrissionPage import ChromiumPage
import re
import pandas as pd
import numpy as np
from utilsByresId import ESClientHolder
import os
import json
# 创建页面对象

"""爬取数据"""


def spiyder_query():
    queryList = []
    page = ChromiumPage()
    page.get(
        "file:///Users/liangzonglin/Downloads/report.html?sort=result&visible=skipped,failed,error,xfailed,xpassed,rerun"
    )
    items = page.eles("@class=log")
    for item in items:
        mth = re.search(r"query:(.*?) ", item.text)
        if mth:
            value = mth.group(1)
            print(value)
            queryList.append(value)
    queryList
    pd.DataFrame(np.array([queryList]).T, columns=["query"]).drop_duplicates().to_csv(
        "querylist.csv", index=False
    )


def dump_version2(date, savePath, days=1):
    """
    通过reqId进行查询，拉取数据
    Args:
        date (_type_): 日期
    """
    # queryList = pd.read_csv("./querylist.csv").values.T.tolist()[0]
    es_client = ESClientHolder(date=date, savePath=savePath)
    es_client.dump_batch(days=days)


class Analysis:
    """_summary_"""

    def __init__(self, date):
        """_summary_
        Args:
            fileDir (_type_): csv file Path
            date (_type_): date
        """
        self.date = date
        self.fileDir = [f"data/task34/{d}" for d in date]

    def analysis(self):
        # queryList = pd.read_csv("./querylist.csv").values.T.tolist()[0]
        # queryDict = []
        filePath = []
        for fileD in self.fileDir:
            fileList = os.listdir(fileD)
            for file in fileList:
                if file.endswith(".json"):
                    filePath.append(f"{fileD}/{file}")
        queryF = open("7days_queryAndResponse.txt", "w")
        for file in filePath:
            with open(file, "r") as f:
                while True:
                    line = f.readline()
                    if line:
                        resList = []
                        msg = json.loads(line)["_source"]["message"]
                        mat = re.search(r"query=(.*?),", msg)
                        if mat:
                            value = mat.group(1)
                            resList.append(value)
                        else:
                            continue
                        startIndex = msg.find("response =")
                        if startIndex != -1:
                            reponse = msg[startIndex + 10 : -1].strip()
                            resList.append(reponse)
                            queryF.write("{}\t{}\n".format(resList[0], resList[1]))
                    else:
                        break
        queryF.close()
        queryAndResponseDf = pd.read_csv("7days_queryAndResponse.txt", sep="\t", header=None)
        queryAndResponseDf.columns = ["query", "response"]
        countDf = queryAndResponseDf.groupby(by="query").count()
        dupDf = queryAndResponseDf.drop_duplicates(subset=["query"], keep="first")
        countDf.reset_index(inplace=True)
        mergeDf = pd.merge(dupDf,countDf, how="inner", on="query")
        mergeDf.columns = ["query", "response", "count"]
        mergeDf.to_csv("query_count.csv", index=False)
        dupNum = mergeDf.loc[mergeDf["count"] >= 2, "count"].sum()
        allTotal = mergeDf["count"].sum()
        print(f"重复数据的占比为：{round(dupNum/allTotal, 3)*100}%")

if __name__ == "__main__":
    # date = "2024-06-07"
    dates = ["2024-06-0" + str(d + 1) for d in range(7)]
    # savePath = f"data/task34/{date}"
    # dump_version2(date=date, savePath=savePath, days=1)
    ana = Analysis(dates)
    ana.analysis()
