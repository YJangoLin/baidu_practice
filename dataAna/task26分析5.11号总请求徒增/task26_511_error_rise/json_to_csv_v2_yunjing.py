"""json转csv文件"""

import csv
import os
from functools import reduce
import json
import time
import datetime
import pandas as pd
import numpy as np
import multiprocessing as mp
from multiprocessing import freeze_support
import sys
import pytz
from dags.config.logging import logging


class JsonToCsv:
    def __init__(self):
        """_summary_"""
        pass

    @staticmethod
    def to_json(line):
        """_summary_

        Args:
            line (_type_): json

        Returns:
            _type_: _description_
        """
        try:
            js = json.loads(line)
        except Exception as e:
            logging.error(f"错误的数据行:{line}")
            return None
        orimsg = js["_source"]["message"]
        midmsg = orimsg[orimsg.find("{") :]
        try:
            msg = json.loads(midmsg)
            # products = msg['user_id'].split('&')
            # msg['fc'] = products[0]
            # msg['pk'] = products[1]
            # msg['ak'] = products[2]
            msg["ts"] = msg["ts"]
            msg["deviceId"] = msg["fc"] + msg["pk"] + msg["ak"]
            # msg.pop('user_id')
        except Exception as e:
            logging.error(f"2次错误的数据行:{midmsg}")
            return None
        ts = msg["ts"]
        # unit 打的日志是 秒时间戳 需要补0 1666238330
        if len(str(ts)) == 10:
            ts = ts * 1000
        d = datetime.datetime.fromtimestamp(
            ts / 1000, tz=pytz.timezone("Asia/Shanghai")
        )
        # 精确到毫秒
        d.astimezone(pytz.timezone("Asia/Shanghai"))
        dt = d.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        msg["ts"] = dt
        return msg


class ContentGen:
    def __init__(self):
        pass

    @staticmethod
    def line_gen(file):
        """_summary_

        Args:
            file (_type_): json

        Yields:
            _type_: _description_
        """
        f = open(file, mode="r", encoding="utf-8")
        while True:
            line = f.readline()
            if line:
                yield line
            else:
                f.close()
                return


class FindFileList:
    def __init__(self, date, root_dir):
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


def _trans(line):
    """_summary_

    Args:
        line (_type_): 一行json数据

    Returns:
        _type_: _description_
    """
    return JsonToCsv.to_json(line)


def _gen(file):
    return ContentGen.line_gen(file)


def start(date, root_dir):
    """_summary_
        入口函数
    Args:
        date (_type_): 日期
        root_dir (_type_): _description_
    """
    # dataf = pd.DataFrame(columns=['version', 'ts', 'seg', 'deviceId', 'fc', 'pk', 'ak', 'logId', 'content'])
    files = FindFileList(date, root_dir).file_list()
    for file in files:
        # 创建多进程但并没有用
        # pool = mp.Pool(processes=mp.cpu_count())
        my_list = []
        # for i in ContentGen.line_gen(file):
        # dataf = dataf.append(pool.map(transtojson, linegen()))
        # for i in pool.map(_trans, ContentGen.line_gen(file)):
        #     if i is not None:
        with open(file, mode="r", encoding="utf-8") as f:
            while True:
                line = f.readline()
                if line:
                    res = _trans(line)  # 提取message中的重要信息转换成json
                    if res is not None:
                        my_list.append(res)
                else:
                    break
        # dataf = pd.DataFrame(my_list, columns=['version', 'ts', 'seg', 'deviceId', 'fc', 'pk', 'ak', 'logId',"messageId", 'content', 'dir', 'dataana'])
        dataf = pd.DataFrame.from_dict(
            my_list, orient="columns"
        )  # from_dict方法用于将dict数据转换为dataframe，key为column
        #  # reduce(lambda pre,line:pre.append(line), mpp.map(transtojson,linegen()),dataf)

        new_file_name = f'{file[:file.rindex(".")]}.csv'
        logging.info(f"new_file_name:{new_file_name}")
        dataf.fillna("", inplace=True)
        if "content" in dataf.columns:
            dataf["content"] = dataf["content"].apply(to_str)  # content
        if "extended" in dataf.columns:
            dataf["extended"] = dataf["extended"].apply(to_str)
        if "dataana" in dataf.columns:
            dataf["dataana"] = dataf["dataana"].apply(to_str)
        dataf.to_csv(new_file_name, encoding="utf-8")


def to_str(x):
    # 将python对象解析为json字符串
    return json.dumps(x, ensure_ascii=False) if x else ""
