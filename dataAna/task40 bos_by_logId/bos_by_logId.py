"""入口函数:统计服务类型为媒体服务的MusicBot的各设备请求数量DAU"""

import os
import re
import glob
import pandas as pd
import numpy as np
from dags.temp.base_temp_es_client_holder import ESClientHolder, file_list
import json
import math
from datetime import datetime, timedelta
from dags.config.logging import logging
from dags.config.constant import root_dir_v2
from dags.bos.bos_client_holder import BosClientHolder


def dump_bos_data(ds):
    """

    :param ds: 时间
    :param index:
    :param kwargs:
    :return:
    """
    logging.info(f"当前运行的北京时间日期:{ds}")
    dir = f"{root_dir_v2}{ds}"
    dir_analyse = f"{root_dir_v2}{ds}/analysis/"
    all_csv = glob.glob(os.path.join(dir, "version_*fc*pk*.csv"))
    full_day_csv = dir_analyse + f"origin-my_index-{ds}.csv.zip"
    # 从bos中拉取当前日志,没办法设置查询条件的
    bos_exists = BosClientHolder(ds, root_dir=root_dir_v2).down_file(
        file_name=f"origin-my_index-{ds}.csv.zip"
    )
    if not bos_exists or not os.path.exists(full_day_csv) or not all_csv:
        logging.info("bos中不存在原始文件:开始从ES中拉取")
    elif bos_exists:
        logging.info("bos中存在原始文件:不需要从ES中拉取")
    elif os.path.exists(full_day_csv):
        logging.info("存在全天完整数据,不需要从ES中拉取")
    else:
        logging.error("不存在全天完整数据，程序运行中断需要重新运行")


def getReData(msg, pattern=r'"logId":"(.*?)",'):
    """匹配message字段中的logId

    Args:
        msg (_type_): message字符串
        pattern (regexp, optional): 匹配的正则函数. Defaults to r'"logId":"(.*?)",'.

    Returns:
        _type_: 提取的logId
    """
    logIdMth = re.search(pattern, msg)
    if logIdMth:
        logId = logIdMth.group(1)
        return logId
    else:
        return None


def getDataBylogId(data_dir):
    reader = pd.read_csv(
        data_dir, compression="zip", chunksize=10000
    )  # chunksize=10000, usecols=["logId", "seg"]
    dataList = []
    for chuck in reader:
        contentDf = chuck
        contentDf.drop(["Unnamed: 0"], axis=1, inplace=True)
        dataList.append(
            contentDf[
                (contentDf["logId"] == "471489D0-5E69-408C-A47C-43D0E01529EA")
                & contentDf["seg"].isin(["ASR2IOT", "IOT2ASR"])
            ]
        )
    pd.concat(dataList, axis=0).to_csv("data.csv")


# 计算指标
if __name__ == "__main__":
    sdate = "2024-06-19"
    # savePath = f"data/version2_0/{sdate}/analysis/origin-my_index-{sdate}.csv.zip"
    # dump_version2(sdate=sdate, edate=edate, savePath=savePath)
    # dump_bos_data(ds=sdate)
    # getDataBylogId(data_dir=savePath)
