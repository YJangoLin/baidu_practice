"""elapse time分析"""

import re
import os
import json
from utils import file_list
from dags.config.logging import logging
import pandas as pd
# from dags.temp.task27_stone_time.stone_time_ana import Analysis as anatime


def match_req_id(line):
    """
    正则匹配res_id
    Args:
        line (_type_): json line
    """
    pattern = r": \[(.*?)\]\[\]"
    match = re.search(pattern, line)
    if match:
        return match.group(1)
    else:
        return None


def get_req_id(data_dir):
    files = sorted(file_list(data_dir, filetype=".json"))
    my_list = [[] for i in range(len(files))]
    count = 0
    for index, file in enumerate(files):
        with open(file, 'r') as f:
            while True:
                line = f.readline()
                if line:
                    reqId = match_req_id(line)
                    if reqId:
                        my_list[index].append(reqId.split("-")[-1])
                    else:
                        continue
                else:
                    break
        # 去重
        my_list[index] = list(set(my_list[index]))
        print(f"data{index} len: {len(my_list[index])}")
        if len(my_list[index]) > 65536:
            print(f"{index} 超出限制")
        count += len(my_list[index])
    print(f"count: {count}")
    # my_list 如果超过65536无法查询，可以分开执行
    return my_list

def dump_version2(date, savePath, days=1):
    from utils import ESClientHolder
    """
    通过reqId进行查询，拉取数据
    Args:
        date (_type_): 日期
    """
    es_client = ESClientHolder(date=date, savePath=savePath)
    es_client.dump_batch(days=days)

def dump_version2_hc(date, savePath, days=1):
    from utils_mz import ESClientHolder
    # reqId = get_req_id(dataDir)
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
        self.cacheFile = "data/task31/cacheData"
        self.dataFile = "data/task31"
        self.df = None  # DataFrame

    def extractREColumn(self, msg, patten):
        resList = []
        # ts
        tss = msg.split(" ", 2)
        ts = tss[0] + " " + tss[1]
        resList.append(ts)
        # reqId
        for pattn in patten.keys():
            mth = re.search(patten.get(pattn), msg)
            if mth:
                value = mth.group(1)
            else:
                return None
            resList.append(value)
        return resList

    def file_list(self, dataPath):
        """_summary_

        Returns:
            _type_: _description_
        """
        dir = dataPath
        filenames = os.listdir(dir)
        files = []
        for name in filenames:
            if name.endswith(".json"):
                file_path = os.path.join(dir, name)
                files.append(file_path)
        logging.info("JSON文件列表" + str(files))
        return files

    def get_csv_file(self, fileDir):
        """提取reqId, ts"""
        patten = {"reqid": r": \[(.*?)\]\[\]"}
        count = 0
        # strCheck = '缓存匹配'
        files = self.file_list(fileDir)
        # files = FindFileList(self.date, self.fileDir).file_list()
        for file in files:
            my_list = []
            print(f"start to switch {file}")
            with open(file, "r") as f:
                while True:
                    line = f.readline()
                    count += 1
                    if line:
                        js = json.loads(line)
                        msg = js["_source"]["message"]
                        resData = self.extractREColumn(msg, patten)
                        if resData is None:
                            continue
                        my_list.append(resData)
                    else:
                        # print(len(my_list))
                        # print("出现错误", line)
                        break
            print(f"finish Analysis {file}, total {len(my_list)}")
            df = pd.DataFrame(
                my_list,
                columns=["ts", "reqId"],
            )
            outfile = file.replace(".json", ".csv")
            df.to_csv(outfile, index=False)
        print(f"all number:{count}")
        

    def analysis_data(self):
        # 转换为csv文件
        # self.get_csv_file()
        filePath = []
        fileList = os.listdir(self.dataFile)
        for file in fileList:
            if file.endswith(".csv"):
                filePath.append(f"{self.dataFile}/{file}")
        dataList = []
        for file in filePath:
            df = pd.read_csv(file)
            df.dropna(axis=0, subset=["reqId"], how="any", inplace=True)
            dataList.append(df)
        datadf = pd.concat(dataList, axis=0, ignore_index=True)
        filePath = []
        fileList = os.listdir(self.cacheFile)
        for file in  fileList:
            if file.endswith(".csv"):
                filePath.append(f"{self.cacheFile}/{file}")
        dataList = []
        for file in filePath:
            df = pd.read_csv(file)
            df.dropna(axis=0, subset=["reqId", "ts"], how='any', inplace=True)
            dataList.append(df)
        cacheDf = pd.concat(dataList, axis=0, ignore_index=True)
        sub_cache_df = datadf[~datadf["reqId"].isin(cacheDf["reqId"])].copy()
        max_req_num = datadf["reqId"].nunique()
        cache_req_num = cacheDf["reqId"].nunique()
        sub_cache_req_num = sub_cache_df["reqId"].nunique()
        print(f"总请求数：{max_req_num}")
        print(f"命中请求数：{cache_req_num}")
        print(f"未命中请求数：{sub_cache_req_num}")
        def getSecond(x):
            return x[:-4]
        sub_cache_df.loc[:, "second"] = sub_cache_df["ts"].apply(getSecond)
        sub_cache_df.drop_duplicates(subset="reqId", keep="first", inplace=True)
        # print(len(sub_cache_df))
        sub_cache_max_qps = sub_cache_df.groupby("second").count()["ts"].max()
        print(f"未命中max_qps: {sub_cache_max_qps}")
        datadf.loc[:, "second"] = datadf["ts"].apply(getSecond)
        max_qps = datadf.groupby("second").count()["ts"].max()
        print(f"总请求的max_qps: {max_qps}")        
        


if __name__ == "__main__":
    # 不要多打空格
    dates = "2024-05-29 15"
    # dump_version2(dates, savePath="data/task31",days=1)
    # dump_version2_hc(dates, savePath="data/task31/CacheData", days=1)
    fileCachePath = "data/task31/CacheData"
    fileDataPath = "data/task31"
    ana = Analysis(date=dates)
    # ana.get_csv_file(fileCachePath)
    # ana.get_csv_file(fileDataPath)
    ana.analysis_data()
