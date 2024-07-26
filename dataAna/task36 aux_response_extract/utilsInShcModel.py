"""es拉取数据"""

import os
import glob
import datetime
import pandas as pd

# from stone_es_dump import StoneBaseDump
# from dags.config.constant import root_dir_v2
from multiprocessing import Process
from dags.config.logging import logging

# import os
import json
from elasticsearch import Elasticsearch
# from dags.config.logging import logging

"""获取文件路径"""
def file_list(dataDir, filetype):
    """_summary_

    Returns:
        _type_: _description_
    """
    dir = dataDir
    filenames = os.listdir(dir)
    files = []
    for name in filenames:
        if name.endswith(filetype):
            file_path = os.path.join(dir, name)
            files.append(file_path)
    logging.info("JSON文件列表" + str(files))
    return files


class StoneBaseDump:
    """
    连接es提取数据
    """

    def __init__(self, sdate, edate, savePath, sandbox=False, index="shc*"):
        self.sdate = sdate
        self.edate = edate
        self.savePath = savePath
        self.es = Elasticsearch(
            hosts="http://10.178.0.67:8200/",
            http_auth=("shc", "HVQMOoG2cRNiQfHQ"),
            timeout=180,
        )
        self.index = index

    def dump(self):
        """
        导出数据,存储到文本文件中 文件路径: ./data/{self.date}/es_origin_data.json
        :return:
        """
        if self._should_pull():
            self._query_data()

            # size = self._check()
            # logging.info(f'file total lines:{size}')

    def _should_pull(self):
        # todo：修改
        file = f"{self.savePath}/es_origin_data_{self.sdate[11:19]}.json"
        return not os.path.exists(file)

    def _query_data(self, limit_fetch=5000):
        """
        这里在从es拉取原始数据时不能直接保存全量再写入，对于内存消耗太大；需要根据Scroll API 每次scroll 查询就把结果保存
        """
        # time.sleep(10)
        # return ['xxx', 'yyyyy']
        query_json = self._get_query_json(self.sdate, self.edate)
        query = self.es.search(
            index=self.index,
            body=query_json,
            scroll="15m",
            timeout="180s",
            size=limit_fetch,
        )
        # print(query)
        # 首次的查询请求，先保存一次
        results = query["hits"]["hits"]
        self._save_first(results)
        total = query["hits"]["total"]["value"]
        scroll_id = query["_scroll_id"]
        # logging.info(f'results:{len(results)}')
        # 后续的滚动请求返回
        for i in range(0, int(total / limit_fetch)):
            query_scroll = self.es.scroll(
                scroll_id=scroll_id,
                scroll="15m",
            )["hits"]["hits"]
            logging.info(f"total lines:{len(query_scroll)}")
            self._save(query_scroll)

    def _save(self, query_scroll):
        # todo修改
        file = f"{self.savePath}/es_origin_data_{self.sdate[11:19]}.json"
        os.makedirs(os.path.dirname(file), exist_ok=True)
        logging.info(f"file_path:{file}")
        with open(file, mode="a", encoding="utf-8") as f:
            for doc in query_scroll:
                f.write(json.dumps(doc) + "\n")

    def _save_first(self, results):
        # todo：修改
        file = f"{self.savePath}/es_origin_data_{self.sdate[11:19]}.json"
        os.makedirs(os.path.dirname(file), exist_ok=True)
        logging.info(f"file_path:{file}")
        with open(file, mode="w", encoding="utf-8") as f:
            for doc in results:
                f.write(json.dumps(doc) + "\n")

    def _get_query_json(self, sdate=None, edate=None):
        logging.info("sdate:" + sdate)
        logging.info("edate:" + edate)
        return self.get_query_dsl()

    def get_query_dsl(self):
        pass


class BaseClientHolder:
    """_summary_"""

    def __init__(self, sdate, edate, savePath):
        self.sdate = sdate
        self.edate = edate
        self.savePath = savePath

    def dump_batch(self, qlists=None, days=1):
        logging.info(f"start time:{datetime.datetime.now()}")
        start = datetime.datetime.now().timestamp()
        date_list = self._split(days)
        result = []
        # 记录切分时间后的子集
        for index, (sdate, edate) in enumerate(date_list):
            # todo:这个地方线程是不是将时间拆分成4段，创建4个线程分别获取，那我应该每次传入qlits[i]
            if qlists:
                dump_process = self.get_dump_process(sdate, edate, qlists)
            else:
                dump_process = self.get_dump_process(sdate, edate, None)
            result.append(dump_process)
        logging.info("prepare process list:" + str(result))
        for p in result:
            p.start()

        # self._check_result(len(result))

        logging.info(f"done time:{(datetime.datetime.now().timestamp() - start)}s")

    def dump_single(self, sdate, edate, qlists=None):
        """

        :param sdate:
        :param edate:
        :return:
        """
        logging.info(f"start time:{datetime.datetime.now()}")
        start = datetime.datetime.now().timestamp()
        dump_process = self.get_dump_process(sdate, edate, qlists[0])
        dump_process.start()
        logging.info(f"done time:{(datetime.datetime.now().timestamp() - start)}s")

    def _check_result(self, count):
        # check files number
        # todo:修改
        dir = f"{self.savePath}/queryByresId"
        all_json = glob.glob(os.path.join(dir, "es_origin_data_*.json"))
        numbers_now = len(all_json)
        if count != numbers_now:
            raise Exception(f"文件下载失败! 实际个数:{numbers_now} ,期待个数:{count}")

    def _split(self, days=1):
        # file_count = 4
        # 开始切分日期
        # strptime 给定字符串返回时间对象
        if len(self.edate) == 10:
            dateFormat = "%Y-%m-%d"
        else:
            dateFormat = "%Y-%m-%d %H:%M:%S"
        start_date = datetime.datetime.strptime(self.sdate, dateFormat)
        end_date = datetime.datetime.strptime(self.edate, dateFormat)
        # strftime 给定时间对象返回字符串
        sdate = start_date.strftime("%Y-%m-%d %H:%M:%S.%f").split(".")[0]
        edate = end_date.strftime("%Y-%m-%d %H:%M:%S.%f").split(".")[0]
        return [(str(sdate), str(edate))]

    def get_dump_process(self, sdate, edate, qlist):
        pass


class DumpProcess(Process):
    """_summary_

    Args:
        Process (_type_): _description_
    """

    def __init__(self, sdate, edate, qlist, savePath):
        Process.__init__(self)
        self.sdate = sdate
        self.edate = edate
        self.qlist = qlist
        self.savePath = savePath

    def run(self):
        dump = Dump(self.sdate, self.edate, self.savePath, self.qlist)
        dump.dump()


class ESClientHolder(BaseClientHolder):
    """_summary_

    Args:
        BaseClientHolder (_type_): _description_
    """

    def __init__(self, sdate, edate, savePath):
        super(ESClientHolder, self).__init__(sdate,edate, savePath)

    def get_dump_process(self, sdate, edate, qlist):
        return DumpProcess(sdate, edate, qlist=qlist, savePath=self.savePath)


class Dump(StoneBaseDump):
    """
    比ES中导出数据
    """

    def __init__(self, sdate, edate, savePath, qlist):
        # !: 主要观察index是_还是-
        super(Dump, self).__init__(sdate, edate, savePath, index="shc_model*")
        self.qlist = qlist

    def get_query_dsl(self):
        return {
            "_source": ["message"],
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": self.sdate,
                                    "lt": self.edate,
                                    "format": "yyyy-MM-dd HH:mm:ss",
                                    "time_zone": "+08:00",
                                }
                            }
                        }
                    ],
                    "must": [
                        {"match_phrase": {"message": "aux_schedule_management"}},
                        {"match_phrase": {"message": "response"}}
                    ],
                }
            },
        }
