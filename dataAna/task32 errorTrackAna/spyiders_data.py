from DrissionPage import ChromiumPage
import re
import pandas as pd
import numpy as np
from utilsByresId import ESClientHolder
import os
import json
# 创建页面对象

def spiyder_query():
    queryList = []
    page = ChromiumPage()
    page.get('file:///Users/liangzonglin/Downloads/report.html?sort=result&visible=skipped,failed,error,xfailed,xpassed,rerun')
    items = page.eles('@class=log')
    for item in items:
        mth = re.search(r"query:(.*?) ",item.text)
        if mth:
            value = mth.group(1)
            print(value)
            queryList.append(value)
    queryList
    pd.DataFrame(np.array([queryList]).T, columns=["query"]).drop_duplicates().to_csv("querylist.csv", index=False)
    
def dump_version2(date, savePath, days=1):
    """
    通过reqId进行查询，拉取数据
    Args:
        date (_type_): 日期
    """
    queryList = pd.read_csv("./querylist.csv").values.T.tolist()[0]
    es_client = ESClientHolder(date=date, savePath=savePath)
    es_client.dump_batch(days=days, qlists=queryList)


class Analysis:
    """_summary_"""

    def __init__(self, date):
        """_summary_
        Args:
            fileDir (_type_): csv file Path
            date (_type_): date
        """
        self.date = date
        self.fileDir = [f"data/task33/{d}" for d in date]

    def analysis(self):
        queryList = pd.read_csv("./querylist.csv").values.T.tolist()[0]
        queryDict = []
        filePath = []
        for fileD in self.fileDir:
            fileList = os.listdir(fileD)
            for file in fileList:
                if file.endswith(".json"):
                    filePath.append(f"{fileD}/{file}")
        for file in filePath:
            with open(file, 'r') as f:
                while True:
                    line = f.readline()
                    if line and len(queryList) != 0:
                        resList = []
                        msg = json.loads(line)["_source"]["message"]
                        tss = msg.split(" ", 2)
                        ts = tss[0] + " " + tss[1]
                        resList.append(ts)
                        mat = re.search(r"query=(.*?),", msg)
                        if mat:
                            value = mat.group(1)
                            if value in queryList:
                                resList.append(value)
                            else:
                                continue
                        startIndex = msg.find("response =")
                        if startIndex != -1:
                            reponse = msg[startIndex+10:-1].strip()
                            resList.append(reponse)
                            queryDict.append(resList)
                            queryList.remove(value)
                    else:
                        break
        pd.DataFrame(np.array(queryDict), columns=["ts", "query", "response"]).to_csv("error_query_info.csv", index=False)


if __name__ == '__main__':
    date="2024-05-31"
    dates = [date] + ["2024-06-0"+str(d+1) for d in range(6)]
    savePath = f"data/task33/{date}"
    # dump_version2(date=date, savePath=savePath, days=1)
    ana = Analysis(dates)
    ana.analysis()
