"""elapse time分析"""

import re
import os
import json
from utilsByresId import file_list, ESClientHolder
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
    my_list = [[], [], [], []]
    for index, file in enumerate(files):
        with open(file, 'r') as f:
            while True:
                line = f.readline()
                if line:
                    reqId = match_req_id(line)
                    if reqId:
                        my_list[index].append(reqId)
                    else:
                        continue
                else:
                    break
        if len(my_list[index]) > 65536:
            print(f"{index} 超出限制")
    # my_list 如果超过65536无法查询，可以分开执行
    return my_list


def dump_version2(date, savePath):
    """
    通过reqId进行查询，拉取数据
    Args:
        date (_type_): 日期
    """

    # qlists = get_log_id(date=date, data_dir=dataDir)
    es_client = ESClientHolder(date=date, savePath=savePath)
    es_client.dump_batch()


def dump_version2_byResId(date, dataDir, savePath):
    reqId = get_req_id(dataDir)
    es_client = ESClientHolder(date=date, savePath=savePath)
    es_client.dump_batch(qlists=reqId)

class Analysis:
    """_summary_"""

    def __init__(self, date):
        """_summary_
        Args:
            fileDir (_type_): csv file Path
            date (_type_): date
        """
        self.date = date
        self.fileDir = [f"data/task29/{d}/queryById" for d in date]
        self.df = None  # DataFrame
        self.saved_dir = f"./{self.date}/temp/"
        self.resColumns = ["std", "mean", "q_50", "q_90", "q_99", "q_999", "q_9999"]
        self.model_index = [
            "ALARM",
            "APP_MANAGEMENT",
            "GREETING",
            "PHONE",
            "ROBOROCK_CLEAN_FUNCTION",
            "ROBOROCK_CLEAN_FUNCTION_MULTI",
            "ROBOROCK_OTHER_FUNCTION",
            "ROBOROCK_PARAMETER_CONTROL",
            "ROBOROCK_SCHEDULE_MANAGEMENT",
            "SPACE",
            "SYS_COMMAND",
            "SYS_PARAMETER",
            "SYS_PERIPHERALS",
            "SYS_PROFILE",
            "TIME",
            "UNIT",
        ]

    def extractREColumn(self, msg):
        startIndex = msg.find("{")
        msg = msg[startIndex:]
        msgJson = json.loads(msg)
        resJson = msgJson["results"]
        query = resJson.get("query", None)
        result = resJson.get("result", None)
        print(len(result))
        if query is None or result is None:
            return None
        content = result[0].get("answer", None)
        faqId = result[0].get("faq_id", None)
        return [query, content, faqId]

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

    def get_csv_file(self):
        strCheck = "FAQ skill response"
        my_list = []
        for index, d in enumerate(self.date):
            files = self.file_list(self.fileDir[index])
            # files = FindFileList(self.date, self.fileDir).file_list()
            for file in files:
                print(f"start to switch {file}")
                with open(file, "r") as f:
                    while True:
                        line = f.readline()
                        if line:
                            js = json.loads(line)
                            msg = js["_source"]["message"]
                            if strCheck not in msg:
                                continue
                            resData = self.extractREColumn(msg)
                            if resData is None:
                                continue
                            my_list.append(resData)
                        else:
                            break
                print(f"finish Analysis {file}, total {len(my_list)}")
                df = pd.DataFrame(
                    my_list,
                    columns=[
                        "query",
                        "content",
                        "faqId"
                    ],
                )
                outfile = file.replace(".json", ".csv")
                df.to_csv(outfile, index=False)


    def analysis_data(self):
        # 转换为csv文件
        # self.get_csv_file()
        filePath = []
        for fileD in self.fileDir:
            fileList = os.listdir(fileD)
            for file in fileList:
                if file.endswith(".csv"):
                    filePath.append(f"{fileD}/{file}")
        dataList = []
        for file in filePath:
            df = pd.read_csv(file)
            df.dropna(
                axis=0,
                subset=["faqId"],
                how="any",
                inplace=True,
            )
            dataList.append(df)
        datadf = pd.concat(dataList, axis=0, ignore_index=True)
        datadf.drop_duplicates(inplace=True)
        print(f"设备数量：{datadf.count()}")
        datadf.to_csv("stone_faq_statistic.csv", index=False)


if __name__ == "__main__":
    dates = ['2024-05-27']
    # dump_version2(dates[0], savePath=f"data/task29/{dates[0]}")
    # dump_version2_byResId(dates[0], savePath=f"data/task29/{dates[0]}/queryById", dataDir=f"data/task29/{dates[0]}")
    ana = Analysis(date=dates)
    ana.analysis_data()
