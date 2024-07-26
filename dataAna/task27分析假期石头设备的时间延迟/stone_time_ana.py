"""主程序入口文件"""

import os
import json
import pandas as pd
import re
from dags.config.constant import root_dir_v2
from dags.config.logging import logging

pd.set_option("expand_frame_repr", False)  # print列全显示


def dump_version2(date):
    from version2_dump_stone import ESClientHolder

    es_client = ESClientHolder(date=date)
    es_client.dump_batch()


def _trans(line):
    """
    正则匹配res_id
    Args:
        line (_type_): json line
    """
    pattern = r"InputAsrFunctionService : \[(.*?)\]"
    match = re.search(pattern, line)
    if match:
        return match.group(1)
    else:
        return ""


class FindFileList:
    """获取文件"""

    def __init__(self, date, root_dir):
        """
            Initializes the class with the given date and root directory.

        Args:
            date (str): The date in the format 'YYYY-MM-DD'.
            root_dir (str): The root directory of the data files.

        Returns:
            None.
        """
        self.date = date
        self.root_dir = root_dir

    def file_list(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        dir = f"{self.root_dir}{self.date}"
        filenames = os.listdir(dir)
        files = []
        for name in filenames:
            if name.endswith(".json"):
                file_path = os.path.join(dir, name)
                files.append(file_path)
        logging.info("JSON文件列表" + str(files))
        return files


def get_res_id(date, data_dir):
    files = FindFileList(date, data_dir).file_list()
    my_list = [[], [], [], []]
    for index, file in enumerate(files):
        with open(file, mode="r", encoding="utf-8") as f:
            while True:
                line = f.readline()
                if line:
                    res = _trans(line)  # 提取message中的重要信息转换成json
                    if res is not None and res != "":
                        my_list[index].append(res)
                else:
                    break
        if len(my_list) > 65536:
            print(f"{index} 超出限制")
    # my_list 如果超过65536无法查询，可以分开执行
    return my_list


def dump_version2_ByresId(date):
    """
    通过reqId进行查询，拉取数据
    Args:
        date (_type_): 日期
    """
    from version2_dump_stoneByreqId import ESClientHolder

    qlists = get_res_id(date=date, data_dir=root_dir_v2)
    count = 0
    for qlist in qlists:
        count += len(set(qlist))
    print(f"总请求数：{count}")
    es_client = ESClientHolder(date=date)
    es_client.dump_batch(qlists)

    """
    先提取每行json数据的ts, seg, resid,  domain, {domain} skill 
    response, (domain) skill request, "reqUnit:UnitRequest", 
    "reqUnit response"
    """


class Analysis:
    """_summary_"""

    def __init__(self, date):
        """_summary_
        Args:
            fileDir (_type_): csv file Path
            date (_type_): date
        """
        self.date = date
        self.fileDir = [f"data/version2_0/{d}/queryByresId" for d in date]
        self.df = None  # DataFrame
        self.saved_dir = f"./{self.date}/temp/"
        self.resColumns = ["std", "mean", "q_50", "q_90", "q_99", "q_999", "q_9999"]
        self.model_index = [
            "all_delay",
            "XIAO_DU",
            "sub_du",
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
            "Unit",
        ]

    def findIndex(self, itemList, item):
        if item in itemList:
            return itemList.index(item)
        else:
            return -1

    def extractREColumn(self, line):
        resList = ["", "", "", "", "", "", False, ""]
        try:
            js = json.loads(line)
        except Exception as e:
            logging.error(f"错误的数据行:{line}")
        msg = js["_source"]["message"]
        resList[1] = js["_source"]["fields"]["region"]
        tss = msg.split(" ", 2)
        ts = tss[0] + " " + tss[1]
        resList[0] = ts
        # Unit: [] reqUnit:UnitRequest(  xx [] reqUnit response:{    xx [] reqUnit response entity:
        # re     r'\[\] reqUnit:UnitRequest('          r'\[\] reqUnit (.*?):{'    r'\[\] reqUnit (.*?) entity'
        exp_domain = [
            "audiomuseum",
            "talk",
            "baike",
            "calculator",
            "deep_qa",
            "exchange_rate",
            "knowledge",
            "traffic_limit",
            "stock",
            "translation",
            "ask_time",
            "image",
            "lottery",
            "unitstatic",
            "universal_search",
            "weather",
            "audio.joke",
            "ask_time",
            "joke",
            "news",
            "failure",
        ]
        cache = ["高频query缓存命中", "必过集缓存命中"]
        patten = {
            "segPatten": r'"seg":"(.*?)"',
            "resid": r": \[(.*?)\]\[\]",
            "domain": r"\[\] (.*?) skill (.*?) \= \{",
            "log_id": r'"logId":"(.*?)",',
        }
        i = 2
        for key in patten.keys():
            match = re.search(patten.get(key), msg)
            if match:
                if key == "domain":
                    value1 = match.group(2)
                    resList[-1] = value1
                value = match.group(1)
            else:
                value = ""
            resList[i] = value
            i += 1
        # 判断是否时Unit
        if resList[4] == "":
            str1 = "[] reqUnit:UnitRequest("
            str2 = "[] reqUnit response:{"
            str3 = "[] reqUnit response entity:"
            if str1 in msg:
                resList[4] = "Unit"
                resList[-1] = "request"
            elif str2 in line or str3 in msg:
                resList[4] = "Unit"
                resList[-1] = "response"
            elif '\\"domain\\":\\"failure\\"' in msg or "domain=failure" in msg:
                resList[4] = "XiaoDu"
                resList[6] = True
        # 检测是否是xiaodu超时
        device_fileter = [
            "roborock_clean_function",
            "roborock_parameter_control",
            "roborock_other_function",
            "roborock_schedule_management",
            "sys_command",
            "sys_parameter",
            "sys_peripheral",
            "alarm",
            "app_management",
            "app_management",
            "phone",
            "greeting",
            "sys_profile",
            "sysprofile",
            "unknown",
        ]
        match = re.search(r"\\\"domain\\\":\\\"(.*?)\\\"", msg)
        if match:
            name = match.group(1)
            # if name in exp_domain:
            #     print("name")
        else:
            match = re.search(r"domain=(.*?),", msg)
            if match:
                name = match.group(1)
                # if name in exp_domain:
                #     print("name")
            else:
                name = ""
        if name in exp_domain:
            resList[6] = True
        if "get highest priority skill service is XiaoDuModelService" in msg:
            resList[6] = True
        # 检查是否缓存
        if (cache[0] in msg) or (cache[1] in msg):
            resList[6] = True

        # 过滤掉一些设备
        if name != "" and self.findIndex(device_fileter, name) != -1:
            resList[6] = False
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
                        "seg",
                        "reqId",
                        "domain",
                        "logId",
                        "subXD",
                        "req",
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
            f"std: {std} \t mean: {mean}\t q_50: {q_5} \t q_90: {q_9} \t q_99: {q_99} \t q_999: {q_999}\t q_9999: {q_9999}"
        )
        return (std, mean, q_5, q_9, q_99, q_999, q_9999)

    def deal_other(self, name, df):
        name_df = df[df["domain"] == name]
        request_df = name_df[name_df["req"] == "request"][["reqId", "ts"]]
        response_df = name_df[name_df["req"] == "response"][["reqId", "ts"]]
        time_df = pd.merge(request_df, response_df, on=["reqId"], how="inner")
        time_df["ts_x"] = pd.to_datetime(time_df["ts_x"])
        time_df["ts_y"] = pd.to_datetime(time_df["ts_y"])
        sub_time = (time_df["ts_y"] - time_df["ts_x"]).dt.total_seconds() * 1000
        return self.compute_q(sub_time)

    def jq_ana(self, jq):
        resData = []
        datadf = self.df
        datadf = datadf[datadf["region"] == jq]
        # 计算总延时
        IOT2ASR_df = datadf[datadf["seg"] == "IOT2ASR"][["reqId", "ts"]]
        ASR2IOT_df = datadf[datadf["seg"] == "ASR2IOT"][["reqId", "ts"]]
        all_time_df = pd.merge(ASR2IOT_df, IOT2ASR_df, on=["reqId"], how="inner")
        all_time_df["ts_x"] = pd.to_datetime(all_time_df["ts_x"])
        all_time_df["ts_y"] = pd.to_datetime(all_time_df["ts_y"])
        all_time_df["duration"] = (
            all_time_df["ts_y"] - all_time_df["ts_x"]
        ).dt.total_seconds() * 1000
        resData.append(self.compute_q(all_time_df["duration"]))

        # 计算xiaodu的响应时间
        IOT2DU_df = datadf[datadf["seg"] == "IOT2DU"][["reqId", "ts"]]
        DU2IOT_df = datadf[datadf["seg"] == "DU2IOT"][["reqId", "ts"]]
        du_time_df = pd.merge(IOT2DU_df, DU2IOT_df, on=["reqId"], how="inner")
        du_time_df["ts_x"] = pd.to_datetime(du_time_df["ts_x"])
        du_time_df["ts_y"] = pd.to_datetime(du_time_df["ts_y"])
        du_time_df["duration"] = (
            du_time_df["ts_y"] - du_time_df["ts_x"]
        ).dt.total_seconds() * 1000
        resData.append(self.compute_q(du_time_df["duration"]))

        # 计算去除xiaodu以后的时间
        sub_req_id = datadf[datadf["subXD"]]["reqId"]
        sub_du_data = all_time_df[~all_time_df["reqId"].isin(sub_req_id)]["duration"]
        resData.append(self.compute_q(sub_du_data))
        # 计算其他指标的时间
        for model in self.model_index[3:]:
            print(model)
            resData.append(self.deal_other(model, datadf))
        # 保存
        resdf = pd.DataFrame(resData, index=self.model_index, columns=self.resColumns)
        resdf.to_excel(f"stone_{jq}_time.xlsx")

    def analysis_data(self):
        # 转换为csv文件
        # self.get_csv_file()
        filePath = []
        for fileD in self.fileDir:
            fileList = os.listdir(fileD)
            for file in fileList:
                if file.endswith(".csv"):
                    filePath.append(f"{fileD}/{file}")
        jq_type = ["BD", "SZ", "GZ"]
        dataList = []
        for file in filePath:
            df = pd.read_csv(file)[
                ["ts", "region", "seg", "reqId", "domain", "logId", "subXD", "req"]
            ]
            df.dropna(axis=0, subset=["reqId", "ts", "region"], how="any", inplace=True)
            df = df[df["region"].isin(jq_type)]
            dataList.append(df)
        datadf = pd.concat(dataList, axis=0, ignore_index=True)
        datadf.drop_duplicates(inplace=True)
        print(f"数据数量：{str(len(datadf))}")
        self.df = datadf
        return datadf

    def start(self):
        self.get_csv_file()
        self.analysis_data()
        jq_type = ["BD", "SZ", "GZ"]
        for jq in jq_type:
            self.jq_ana(jq=jq)

        # self.analysis_data(df)


if __name__ == "__main__":
    # 116490， 118710
    dates = ["2024-05-18", "2024-05-19"]
    # for date in dates:
    # dump_version2(date=dates[1])
    # dump_version2_ByresId(dates[1])
    ana = Analysis(date=dates)
    ana.get_csv_file()

    # ana.start()
