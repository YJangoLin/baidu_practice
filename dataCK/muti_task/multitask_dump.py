"""多任务es配置文件"""
import os
import glob
import multiprocessing as mp
from datetime import datetime, timedelta
import pandas as pd
from dags.config.logging import logging

import json
from elasticsearch import Elasticsearch


class BaseClientHolder:
    """ 拉取数据的基础类 """
    def __init__(self, date):
        self.date = date

    def dump_batch(self):
        logging.info('start date:' + self.date)
        start_date = datetime.strptime(self.date, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        sdate = start_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-7]
        edate = end_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-7]
        dump = BaseDump(sdate, edate)
        results = dump.dump()
        return results


class BaseDump:
    """
    比ES中导出数据
    """

    def __init__(self, sdate, edate, index='shc-java*'):
        self.sdate = sdate
        self.edate = edate
        self.es = Elasticsearch(hosts='http://10.178.0.67:8200/', http_auth=('shc', 'HVQMOoG2cRNiQfHQ'),
                                timeout=180)
        self.index = index

    def dump(self):
        results = self._query_data()
        logging.info(f'total lines:{len(results)}')
        return results

    def _query_data(self, limit_fetch=5000):
        query_json = self.get_query_dsl()
        query = self.es.search(index=self.index, body=query_json, scroll='15m', timeout='180s', size=limit_fetch, )
        results = query['hits']['hits']
        total = query['hits']['total']['value']
        scroll_id = query['_scroll_id']
        logging.info(f'results:{len(results)}')
        for i in range(0, int(total / limit_fetch)):
            query_scroll = self.es.scroll(scroll_id=scroll_id, scroll='15m', )['hits']['hits']
            results += query_scroll
            logging.info(f'results:{len(results)}')
        return results

    def get_query_dsl(self):
        return {
            "_source": [
                "message"
            ],
            "query": {
                "bool": {
                    "filter":
                        [
                            {
                                "range":
                                    {
                                        "@timestamp":
                                            {
                                                "gte": self.sdate,
                                                "lt": self.edate,
                                                "format": "yyyy-MM-dd HH:mm:ss",
                                                "time_zone": "+08:00"
                                            }
                                    }
                            },
                        ],
                    "must": [{"match_phrase": {"message": "IOT2ASR"}},
                             {"match_phrase": {"message": "query"}},
                             {"match_phrase": {"message": "intent"}},
                             {"match_phrase": {"message": "domain"}},
                             {"match_phrase": {"message": "pk"}}
                             ]
                }
            }
        }
