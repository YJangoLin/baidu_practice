"""es基础类"""

import os
import json
from elasticsearch import Elasticsearch
from dags.config.logging import logging


class StoneBaseDump:
    """
    比ES中导出数据
    """

    def __init__(self, sdate, edate, root_dir, sandbox=False, index="shc*"):
        self.sdate = sdate
        self.edate = edate
        self.root_dir = root_dir
        if sandbox:
            self.es = Elasticsearch(
                hosts="http://100.66.131.46:8200",
                http_auth=("shc", "RnbDHcRVbtx7"),
                timeout=180,
            )
        else:
            curr_date = self.sdate[:10]
            if curr_date >= "2023-02-08":  # 这里切换新版ELK
                self.es = Elasticsearch(
                    hosts="http://10.178.0.67:8200/",
                    http_auth=("shc", "HVQMOoG2cRNiQfHQ"),
                    timeout=180,
                )
            else:
                self.es = Elasticsearch(
                    hosts="http://100.66.129.133:8200/",
                    http_auth=("shc", "lvMt4loWb_sC"),
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
        file = f"{self.root_dir}{self.sdate[:10]}/queryByresId/es_origin_data_{self.sdate[11:19]}.json"
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
        file = f"{self.root_dir}{self.sdate[:10]}/queryByresId/es_origin_data_{self.sdate[11:19]}.json"
        os.makedirs(os.path.dirname(file), exist_ok=True)
        logging.info(f"file_path:{file}")
        with open(file, mode="a", encoding="utf-8") as f:
            for doc in query_scroll:
                f.write(json.dumps(doc) + "\n")

    def _save_first(self, results):
        # todo：修改
        file = f"{self.root_dir}{self.sdate[:10]}/queryByresId/es_origin_data_{self.sdate[11:19]}.json"
        os.makedirs(os.path.dirname(file), exist_ok=True)
        logging.info(f"file_path:{file}")
        with open(file, mode="w", encoding="utf-8") as f:
            for doc in results:
                f.write(json.dumps(doc) + "\n")

    def _get_query_json(self, sdate=None, edate=None):
        # if sdate is None or edate is None:
        #     start_date = datetime.datetime.strptime(self.date, '%Y-%m-%d').date()
        #     end_date = start_date + datetime.timedelta(days=1)
        #     sdate = start_date.strftime('%Y-%m-%d %H:%M:%S')
        #     edate = end_date.strftime('%Y-%m-%d %H:%M:%S')
        logging.info("sdate:" + sdate)
        logging.info("edate:" + edate)
        #     # sdate = '2022-07-25T05:00:00'
        #     # edate = '2022-07-25T05:30:00'
        return self.get_query_dsl()

    def get_query_dsl(self):
        pass
