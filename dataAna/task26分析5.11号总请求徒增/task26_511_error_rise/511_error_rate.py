"""主程序入口文件"""

import os
import pandas as pd
import re
from dags.config.constant import root_dir_v2

pd.set_option("expand_frame_repr", False)  # print列全显示


def dump_yunjing_v2(date):
    from version2_dump_yunjing import ESClientHolder

    es_client = ESClientHolder(date="2024-05-11")
    es_client.dump_batch(date)


def to_yunjing_json_v2(date):
    from json_to_csv_v2_yunjing import start

    start(date, root_dir_v2)


class Analysis:
    """_summary_
    """
    
    def __init__(self, fileDir, date):
        """_summary_
        Args:
            fileDir (_type_): csv file Path
            date (_type_): date
        """
        self.date = date
        self.fileDir = fileDir
        self.df = None  # DataFrame
        self.saved_dir = f"./{self.date}/temp/"

    def loadData(self):
        """
        加载整理好的csv文件
        Returns:
            _type_: _description_
        """
        # data_path = "/Users/liangzonglin/workplace/project/baidu/bce-iot/du-home-log/data/version2_0/2024-05-11"
        fileNames = os.listdir(self.fileDir)
        CSVNames = [fileName for fileName in fileNames if fileName.endswith(".csv")]
        dataList = []
        for fileName in CSVNames:
            datadf = pd.read_csv(self.fileDir + "/" + fileName)
            dataList.append(datadf)
        df = pd.concat(dataList, axis=0, ignore_index=True)
        self.df = df
        return df

    """
        截取ts到小时
    """

    def timeshort(self, tstr):
        """_summary_

        Args:
            tstr (_type_): _description_

        Returns:
            _type_: 截取ts到小时
        """
        return tstr[:-10]

    """
        提取content和dataana列中的query数据，构造query列
    """

    def extractQuery(self, x):
        """_summary_

        Args:
            x (_type_): 数据行

        Returns:
            _type_: _description_
        """
        contentJsonStr = x["content"]
        datanaJsonStr = x["dataana"]
        pattern = r'"query": "(.*?)"'
        match, match1 = None, None
        if pd.notna(contentJsonStr):
            match = re.search(pattern, contentJsonStr)
        if pd.notna(datanaJsonStr):
            match1 = re.search(pattern, datanaJsonStr)
        if match:
            name = match.group(1)
            return name
        elif match1:
            name = match1.group(1)
            return name
        else:
            return ""

    """
        分析徒增现象的原因生成之间数据。
    """

    def ana_AUTH2ASR(self, df):
        """_summary_

        Args:
            df (_type_): _description_
        """
        ana_DAU = df["deviceId"].nunique()
        authcount = df.query('seg=="AUTH2ASR"').shape[0]
        unitcount = df.query('seg=="ASR2IOT"').shape[0]
        print("2024-5-11:")
        print(f"DAU:{str(ana_DAU)}")
        print(f"总请求：{str(authcount)}")
        print(f"有效请求：{str(unitcount)}")
        # 构造query
        self.df["query"] = self.df.apply(self.extractQuery, axis=1)
        # 获取总请求前10个device的信息
        device_request_top10 = (
            df[df["seg"] == "AUTH2ASR"][["deviceId", "seg"]]
            .groupby("deviceId")
            .count()
            .sort_values("seg", ascending=False)[:10]
        )
        device_request_top10["request_rate"] = device_request_top10["seg"] / authcount
        device_request_top10.columns = ["request_num", "request_rate"]
        device_index = device_request_top10.index
        devicetop10df = df[df["deviceId"].isin(device_index)][df["seg"] == "ASR2IOT"]
        deviceASRNum = devicetop10df.groupby("deviceId").count()
        deviceASRNum["request_unit_rate"] = deviceASRNum["seg"] / unitcount
        deviceUintInfo = deviceASRNum[["seg", "request_unit_rate"]]
        deviceUintInfo.columns = ["unit_num", "request_unit_rate"]
        merge_df = pd.merge(
            device_request_top10, deviceUintInfo, left_index=True, right_index=True
        )
        merge_df.to_csv(f"{self.saved_dir}device_all_request_ana.csv")
        # 把这几个设备的信息输出
        df[df["deviceId"].isin(device_index[:7])][
            ["deviceId", "fc", "pk", "ak"]
        ].groupby("deviceId").first().loc[device_index[:7], :].to_excel(
            "error_device_tuple.xlsx"
        )
        # 分析异常设备时间维度的是否异常
        # 选取Top1根据时间分组查看请求变化
        import matplotlib.pyplot as plt

        ana_time = df[(df["seg"] == "AUTH2ASR")].copy()
        ana_time["ts"] = ana_time["ts"].apply(self.timeshort)
        for index, deviceId in enumerate(device_index[:3]):
            deviceId_top1 = device_index[0]
            top_df = (
                ana_time[ana_time["deviceId"] == deviceId_top1]
                .groupby("ts")
                .count()["deviceId"]
            )
            top_df.plot(x="time", y="request_num", kind="line")
            top_df.to_csv(f"{self.saved_dir}fb_top_{str(index+1)}.csv")
            print(f"{deviceId} 方差:{top_df.std()}")

    def analysis(self):
        """
        _summary_
        """
        df = self.oadData()
        self.ana_AUTH2ASR(df)


if __name__ == "__main__":
    date = "2024-5-11"
    fileDir = f"data/version_2_0/{date}"
    # download json
    dump_yunjing_v2(date)
    # transfrom to csv
    to_yunjing_json_v2(date)
    # 创建分析对象,
    ana = Analysis(fileDir=fileDir, date=date)
    ana.analysis()
