"""根据请求id查询es文件"""

import os
import glob
import datetime
import pandas as pd
from stone_es_dump import StoneBaseDump
from dags.config.constant import root_dir_v2
from multiprocessing import Process
from dags.config.logging import logging


class BaseClientHolder:
    """_summary_"""

    def __init__(self, date, root_dir):
        self.date = date
        self.root_dir = root_dir

    def dump_batch(self, qlists):
        logging.info(f"start time:{datetime.datetime.now()}")
        start = datetime.datetime.now().timestamp()
        date_list = self._split()
        result = []
        # 记录切分时间后的子集
        for index, (sdate, edate) in enumerate(date_list):
            # todo:这个地方线程是不是将时间拆分成4段，创建4个线程分别获取，那我应该每次传入qlits[i]
            dump_process = self.get_dump_process(sdate, edate, qlists[index])
            result.append(dump_process)
        logging.info("prepare process list:" + str(result))
        for p in result:
            p.start()

        self._check_result(len(result))

        logging.info(f"done time:{(datetime.datetime.now().timestamp() - start)}s")

    def dump_single(self, sdate, edate, qlists):
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
        dir = f"{self.root_dir}{self.date}/queryByresId"
        all_json = glob.glob(os.path.join(dir, "es_origin_data_*.json"))
        numbers_now = len(all_json)
        if count != numbers_now:
            raise Exception(f"文件下载失败! 实际个数:{numbers_now} ,期待个数:{count}")

    def _split(self, file_count=4):
        # file_count = 4  # cpu_count * LOAD_FACTOR
        # 开始切分日期
        # strptime 给定字符串返回时间对象
        start_date = datetime.datetime.strptime(self.date, "%Y-%m-%d").date()
        end_date = start_date + datetime.timedelta(days=1)
        # strftime 给定时间对象返回字符串
        sdate = start_date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        edate = end_date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        bins = pd.date_range(
            start=sdate, end=edate, freq=f"{24 * 60 / file_count}min"
        ).astype(str)
        res = [x for x in zip(bins[:-1], bins[1:])]
        logging.info("split res: " + str(res))
        return res

    def get_dump_process(self, sdate, edate, qlist):
        pass


class DumpProcess(Process):
    """_summary_

    Args:
        Process (_type_): _description_
    """

    def __init__(self, sdate, edate, qlist):
        Process.__init__(self)
        self.sdate = sdate
        self.edate = edate
        self.qlist = qlist

    def run(self):
        dump = Dump(self.sdate, self.edate, root_dir_v2, self.qlist)
        dump.dump()


class ESClientHolder(BaseClientHolder):
    """_summary_

    Args:
        BaseClientHolder (_type_): _description_
    """

    def __init__(self, date):
        super(ESClientHolder, self).__init__(date, root_dir_v2)

    def get_dump_process(self, sdate, edate, qlist):
        return DumpProcess(sdate, edate, qlist=qlist)


class Dump(StoneBaseDump):
    """
    比ES中导出数据
    """

    def __init__(self, sdate, edate, root_dir, qlist):
        super(Dump, self).__init__(sdate, edate, root_dir, index="shc-java*")
        self.qlist = qlist

    def get_query_dsl(self):
        return {
            "_source": ["fields.region", "message"],
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
                    "must": [{"terms": {"message": self.qlist}}],
                }
            },
        }
