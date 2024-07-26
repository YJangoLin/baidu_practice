"""从测试文档html文件中，爬取错误日志中的query并分析"""

from DrissionPage import ChromiumPage
import re
import pandas as pd
import numpy as np
from utilsByresId import ESClientHolder, file_list
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






def dump_version2(sdate, edate, savePath, days=1):
    """
    通过reqId进行查询，拉取数据
    Args:
        date (_type_): 日期
    """
    # queryList = pd.read_csv("./querylist.csv").values.T.tolist()[0]
    es_client = ESClientHolder(sdate=sdate,edate=edate, savePath=savePath)
    es_client.dump_batch()


def compute_q(serise):
    mean = round(serise.mean(), 1)
    std = round(serise.std(), 1)
    q_5 = round(serise.quantile(0.5, interpolation="lower"), 1)
    q_9 = round(serise.quantile(0.9, interpolation="lower"), 1)
    q_99 = round(serise.quantile(0.99, interpolation="lower"), 1)
    q_999 = round(serise.quantile(0.999, interpolation="lower"), 1)
    q_9999 = round(serise.quantile(0.9999, interpolation="lower"), 1)
    print(f"std: {std} \t mean: {mean}\t q_50: {q_5} \t q_90: {q_9} \t q_99: {q_99} \t q_999: {q_999}\t q_9999: {q_9999}")
    return (std, mean, q_5, q_9, q_99, q_999, q_9999)



def get_req_id(data_dir):
    files = sorted(file_list(data_dir, filetype=".json"))
    my_list = [[] for i in range(len(files))]
    count = 0
    for index, file in enumerate(files):
        with open(file, 'r') as f:
            while True:
                line = f.readline()
                if line:
                    js = json.loads(line)
                    msg = js["_source"]["message"]
                    mat = re.search(r"elapse_time=([-+]?[0-9]*.?[0-9]+)", msg)
                    if mat:
                        my_list[index].append(mat.group(1))
                        count +=1
                    else:
                        continue
                else:
                    break
    print(f"count: {count}")
    df = pd.DataFrame(np.array(my_list).T, columns=["elapse_time"])
    df["elapse_time"] =df["elapse_time"].astype("double")
    df = df.sort_values(by="elapse_time", ascending=False)
    # my_list 如果超过65536无法查询，可以分开执行
    df.to_csv("data.csv", index=False)
    print(len(df))
    print(df.sort_values(by="elapse_time", ascending=False)[:5])
    return compute_q(df["elapse_time"].astype("double"))


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
            with open(file, "r") as f:
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
                            reponse = msg[startIndex + 10 : -1].strip()
                            resList.append(reponse)
                            queryDict.append(resList)
                            queryList.remove(value)
                    else:
                        break
        pd.DataFrame(np.array(queryDict), columns=["ts", "query", "response"]).to_csv(
            "error_query_info.csv", index=False
        )


if __name__ == "__main__":
    sdate = "2024-06-07"
    savePath = "data/task33"
    # dump_version2(sdate=sdate, edate=edate, savePath=savePath)
    # ana = Analysis(dates)
    # ana.analysis()
    get_req_id(savePath)
