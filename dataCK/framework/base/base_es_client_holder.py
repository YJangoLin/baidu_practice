import os
import glob
import multiprocessing as mp
import datetime
import pandas as pd
from base.logging import logging
from conf.config import WORK_NUM


class BaseClientHolder:

    def __init__(self, startDate, endDate, root_dir, file_count=4):
        self.startDate = startDate
        self.endDate = endDate
        self.root_dir = root_dir
        self.file_count = file_count

    def dump_batch(self):
        logging.info(f'start time:{datetime.datetime.now()}')
        start = datetime.datetime.now().timestamp()
        date_list = self._split(WORK_NUM)
        result = []
        # 记录切分时间后的子集
        for sdate, edate in date_list:
            dump_process = self.get_dump_process(sdate, edate)
            result.append(dump_process)
        logging.info('prepare process list:' + str(result))
        for p in result:
            p.start()

        self._check_result(len(result))

        logging.info(f'done time:{(datetime.datetime.now().timestamp() - start)}s')

    def _check_result(self, count):
        # check files number
        dir = f'{self.root_dir}{self.date}'
        all_json = glob.glob(os.path.join(dir, 'es_origin_data_*.json'))
        numbers_now = len(all_json)
        if count != numbers_now:
            raise Exception(f'文件下载失败! 实际个数:{numbers_now} ,期待个数:{count}')

    def _split(self, file_count=4):
        # file_count = 4  # cpu_count * LOAD_FACTOR
        # 开始切分日期
        # strptime 给定字符串返回时间对象
        start_date = datetime.datetime.strptime(self.startDate, '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(self.endDate, '%Y-%m-%d').date()
        # strftime 给定时间对象返回字符串
        sdate = start_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        edate = end_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        # todo: 看看这一块如何修改
        bins = pd.date_range(start=sdate, end=edate, freq=f'{24 * 60 / file_count}min').astype(str)
        res = [x for x in zip(bins[: -1], bins[1:])]
        logging.info('split res: ' + str(res))
        return res

    def get_dump_process(self, sdate, edate):
        pass
