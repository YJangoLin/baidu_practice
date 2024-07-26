"""调用es文件"""

from dags.es.base.base_es_dump import BaseDump
from dags.es.base.base_es_client_holder import BaseClientHolder
from dags.config.constant import root_dir_v2
from multiprocessing import Process


class DumpProcess(Process):
    """_summary_

    Args:
        Process (_type_): _description_
    """

    def __init__(self, sdate, edate):
        Process.__init__(self)
        self.sdate = sdate
        self.edate = edate

    def run(self):
        dump = Dump(self.sdate, self.edate, root_dir_v2)
        dump.dump()


class ESClientHolder(BaseClientHolder):
    """_summary_

    Args:
        BaseClientHolder (_type_): _description_
    """

    def __init__(self, date):
        super(ESClientHolder, self).__init__(date, root_dir_v2)

    def get_dump_process(self, sdate, edate):
        return DumpProcess(sdate, edate)


class Dump(BaseDump):
    """
    比ES中导出数据
    """

    def __init__(self, sdate, edate, root_dir):
        super(Dump, self).__init__(sdate, edate, root_dir, index="shc-java*")

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
                        },
                        {"term": {"message": "z4863s"}},
                    ]
                }
            },
        }
