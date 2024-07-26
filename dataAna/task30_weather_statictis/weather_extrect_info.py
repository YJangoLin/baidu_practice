"""elapse time分析"""

import re
import os
import json
from utils import file_list, ESClientHolder
from dags.config.logging import logging
import pandas as pd
# from dags.temp.task27_stone_time.stone_time_ana import Analysis as anatime


def dump_version2(date, savePath):
    """
    通过reqId进行查询，拉取数据
    Args:
        date (_type_): 日期
    """

    # qlists = get_log_id(date=date, data_dir=dataDir)
    es_client = ESClientHolder(date=date, savePath=savePath)
    es_client.dump_batch()


class Analysis:
    """_summary_"""

    def __init__(self, date):
        """_summary_
        Args:
            fileDir (_type_): csv file Path
            date (_type_): date
        """
        self.date = date
        self.fileDir = [f"data/task30/{d}" for d in date]
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

    def extractREColumn(self, msg, patten, strCheck):
        resList = []
        # ts
        tss = msg.split(" ", 2)
        ts = tss[0] + " " + tss[1]
        resList.append(ts)
        if strCheck in msg:
            return None
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

    def get_csv_file(self):
        """提取reqId, ts"""
        my_list = []
        patten = {"resid": r": \[(.*?)\]\[\]", "deviceId": r'"deviceId":"(.*?)"'}
        strCheck = '"slots":{"geo"'
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
                            resData = self.extractREColumn(msg, patten, strCheck)
                            if resData is None:
                                continue
                            my_list.append(resData)
                        else:
                            break
                print(f"finish Analysis {file}, total {len(my_list)}")
                df = pd.DataFrame(
                    my_list,
                    columns=["ts", "reqId", "deviceId"],
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
            df.dropna(axis=0, subset=["reqId"], how="any", inplace=True)
            dataList.append(df)
        datadf = pd.concat(dataList, axis=0, ignore_index=True)
        datadf["day"] = datadf["ts"].apply(lambda x: x.split(" ")[0])
        datadf["second"] = datadf["ts"].apply(lambda x: x[:-4])
        datadf.drop("ts", axis=1, inplace=True)
        count_df = datadf.drop(["reqId", "second"], axis=1)
        count_df = count_df.drop_duplicates(keep="first")
        count_num_df = count_df.groupby("day").count()
        secondDf = datadf[["reqId", "second"]].drop_duplicates(keep="first")
        qps = secondDf.groupby("second").count()
        qps = qps.reset_index()
        qps["day"] = qps["second"].apply(lambda x: x.split(" ")[0])
        max_qps = qps.groupby("day").max("reqId")
        merge_df = pd.merge(count_num_df, max_qps, left_index=True, right_index=True)
        merge_df.columns = ["请求数", "max_qps"]
        merge_df.index.name = "日期"
        merge_df.to_csv("qps_weather_ana.csv")


if __name__ == "__main__":
    dates = ["2024-05-" + str(28 - i) for i in range(7)]
    # dump_version2(dates[5], savePath=f"data/task30/{dates[5]}")
    # dump_version2_byResId(dates[0], savePath=f"data/task29/{dates[0]}/queryById", dataDir=f"data/task29/{dates[0]}")
    ana = Analysis(date=dates)
    ana.get_csv_file()
    # ana.analysis_data()
