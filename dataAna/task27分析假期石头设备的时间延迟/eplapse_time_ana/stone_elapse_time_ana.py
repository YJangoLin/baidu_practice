"""elapse time分析"""

import re
import os
import json
from utils import file_list, ESClientHolder
from dags.config.logging import logging
import pandas as pd
# from dags.temp.task27_stone_time.stone_time_ana import Analysis as anatime


def match_log_id(line):
    """
    正则匹配res_id
    Args:
        line (_type_): json line
    """
    pattern = r'"logId":"(.*?)",'
    match = re.search(pattern, line)
    if match:
        return match.group(1)
    else:
        return ""


def get_log_id(date, data_dir):
    files = sorted(file_list(data_dir, filetype=".csv"))
    my_list = [[], [], [], []]
    for index, file in enumerate(files):
        logId_df = pd.read_csv(file)["logId"].dropna().drop_duplicates()
        my_list[index] = logId_df.to_list()
        if len(my_list) > 65536:
            print(f"{index} 超出限制")
    # my_list 如果超过65536无法查询，可以分开执行
    return my_list


def dump_version2_ByresId(date, savePath):
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
        self.fileDir = [f"data/version2_0/{d}/el_time" for d in date]
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

    def extractREColumn(self, line):
        resList = ["", "", "", "", "", ""]
        try:
            js = json.loads(line)
        except Exception as e:
            logging.error(f"错误的数据行:{line}")
        msg = js["_source"]["message"]
        resList[1] = js["_source"]["fields"]["region"]
        resList[2] = js["_source"]["fields"]["module"]
        tss = msg.split(" ", 2)
        ts = tss[0] + " " + tss[1]
        resList[0] = ts
        # Unit: [] reqUnit:UnitRequest(  xx [] reqUnit response:{    xx [] reqUnit response entity:
        # re     r'\[\] reqUnit:UnitRequest('          r'\[\] reqUnit (.*?):{'    r'\[\] reqUnit (.*?) entity'
        patten = {
            "log_id": r"INFO - log_id=(.*?),",
            "domain": r"'domain': '(.*?)',",
            "elapse_time": r"elapse_time=(.*?);",
        }
        i = 3
        for key in patten.keys():
            match = re.search(patten.get(key), msg)
            if match:
                value = match.group(1)
            else:
                value = ""
            resList[i] = value
            i += 1
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
        for index, d in enumerate(self.date):
            files = self.file_list(self.fileDir[index])
            # files = FindFileList(self.date, self.fileDir).file_list()
            for file in files:
                print(f"start to switch {file}")
                my_list = []
                with open(file, "r") as f:
                    while True:
                        line = f.readline()
                        if line:
                            res = self.extractREColumn(
                                line
                            )  # 提取message中的重要信息转换成json
                            if res is not None:
                                my_list.append(res)
                        else:
                            break
                print(f"finish Analysis {file}, total {len(my_list)}")
                df = pd.DataFrame(
                    my_list,
                    columns=[
                        "ts",
                        "region",
                        "module",
                        "log_id",
                        "domain",
                        "elapse_time",
                    ],
                )
                outfile = file.replace(".json", ".csv")
                df.to_csv(outfile, index=False)

    def compute_q(self, serise):
        mean = round(serise.mean(), 1)
        std = round(serise.std(), 1)
        q_5 = round(serise.quantile(0.5, interpolation="lower"), 1)
        q_9 = round(serise.quantile(0.9, interpolation="lower"), 1)
        q_99 = round(serise.quantile(0.99, interpolation="lower"), 1)
        q_999 = round(serise.quantile(0.999, interpolation="lower"), 1)
        q_9999 = round(serise.quantile(0.9999, interpolation="lower"), 1)
        print(
            f"std: {std} \t mean: {mean}\t q_50: {q_5} \t q_90: {q_9} \
            \t q_99: {q_99} \t q_999: {q_999}\t q_9999: {q_9999}"
        )
        return (std, mean, q_5, q_9, q_99, q_999, q_9999)

    def deal_other(self, name: str, df, column="domain"):
        if column == "module":
            name_df = df[df[column].str.contains(name.lower().replace("_", "-"))]
        else:
            name_df = df[df[column] == name]
        sub_time = name_df["elapse_time"]
        return self.compute_q(sub_time)

    def jq_ana(self, jq):
        """
            按照数据地区分析
        Args:
            jq (_type_): 集群地区
        """
        resData = []
        datadf = self.df
        datadf = datadf[datadf["region"] == jq]

        # 计算其他指标的时间
        for model in self.model_index:
            print(model)
            resData.append(self.deal_other(model.lower(), datadf))
        # 保存
        resdf = pd.DataFrame(resData, index=self.model_index, columns=self.resColumns)
        resdf.to_excel(f"./stone_elapse_{jq}_time.xlsx")

    def analysis_data(self):
        # 转换为csv文件
        # self.get_csv_file()
        filePath = []
        for fileD in self.fileDir:
            fileList = os.listdir(fileD)
            for file in fileList:
                if file.endswith(".csv"):
                    filePath.append(f"{fileD}/{file}")
        jq_type = ["BJ", "SZ", "GZ"]
        dataList = []
        for file in filePath:
            df = pd.read_csv(file)
            df.dropna(
                axis=0,
                subset=["log_id", "elapse_time", "region"],
                how="any",
                inplace=True,
            )
            df = df[df["region"].isin(jq_type)]
            dataList.append(df)
        datadf = pd.concat(dataList, axis=0, ignore_index=True)
        # datadf.loc[datadf["module"]=="nlu-infer-dreame-multi-task", "domain"] = "ROBOROCK_CLEAN_FUNCTION_MULTI"
        datadf.loc[datadf["module"] == "nlu-infer-sys-peripheral", "module"] = (
            "nlu-infer-sys-peripherals"
        )
        print(f"总数据数量：{str(len(datadf))}")
        datadf.drop_duplicates(inplace=True)
        datadf.dropna(subset=["elapse_time", "region"], inplace=True)
        print(f"去重后以及处理空缺数据量：{str(len(datadf))}")
        self.df = datadf
        return datadf, jq_type

    def start(self):
        self.get_csv_file()
        _, jq_type = self.analysis_data()
        for jq in jq_type:
            self.jq_ana(jq=jq)


if __name__ == "__main__":
    dates = ["2024-05-18", "2024-05-19"]
    # dump_version2_ByresId(date=dates[1], savePath=f"data/version2_0/{dates[1]}/el_time")
    ana = Analysis(date=dates)
    ana.get_csv_file()
    # anat = anatime(dates)
    # time_df = anat.analysis_data()
    # time_df.loc[time_df["region"] == "BD", "region"] = "BJ"
    # eladf, jq_type = ana.analysis_data()
    # model_index = ana.model_index
    # resColumns = ana.resColumns
    # time_df.dropna(subset=["logId"], inplace=True)
    # eladf.dropna(subset=["log_id"], inplace=True)
    # mergedf = pd.merge(
    #     time_df,
    #     eladf,
    #     left_on=["logId", "region"],
    #     right_on=["log_id", "region"],
    #     how="inner",
    # )
    # mergedf.to_csv("./merge.csv", index=False)
    # # mergedf[mergedf["domain_x"].isna()]["domain_x"] = mergedf[mergedf["domain_x"].isna()]["domain_y"]
    # mergedf["domain_y"] = mergedf["domain_y"].str.upper()
    # for jq in jq_type:
    #     resData = []
    #     datadf = mergedf[mergedf["region"] == jq]
    #     # 计算其他指标的时间
    #     for model in model_index:
    #         print(model)
    #         resData.append(ana.deal_other(model, datadf, column="module"))
    #     # 保存
    #     resdf = pd.DataFrame(resData, index=model_index, columns=resColumns)
    #     resdf.to_excel(f"./stone_elapse_{jq}_time.xlsx")
