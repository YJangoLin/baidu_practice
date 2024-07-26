from base.base_es_dump import BaseDump
from base.base_es_client_holder import BaseClientHolder
from conf.config import DATA_ROOR_DIR,  es_query_dict, IS_DYNAMIC_QUERY, WORK_NUM,QUERY_CLUSTER


class DumpProcess:
    """
    多线程导出数据
    """
    def __init__(self, sdate, edate):
        self.sdate = sdate
        self.edate = edate

    def start(self):
        dump = Dump(self.sdate, self.edate, DATA_ROOR_DIR)
        dump.dump()


class ESClientHolder(BaseClientHolder):
    """_summary_

    Args:
        BaseClientHolder (_type_): _description_
    """

    def __init__(self, sdate, edate, savePath):
        
        super(ESClientHolder, self).__init__(sdate, edate, savePath, file_count=WORK_NUM)

    def get_dump_process(self, sdate, edate):
        return DumpProcess(sdate, edate)

class Dump(BaseDump):
    """
    比ES中导出数据
    """

    def __init__(self, sdate, edate, root_dir):
        super(Dump, self).__init__(sdate, edate, root_dir, index=QUERY_CLUSTER)

    # 
    def get_query_dsl(self, extra_query=None, func=None):
        if IS_DYNAMIC_QUERY and extra_query is not None and func is not None:
            es_query = func(es_query_dict, extra_query) # 需要额外重程序中添加的条件
        else:
            es_query = es_query_dict
        es_query["query"]["bool"]["filter"][0]["range"]["@timestamp"]["gte"] = self.sdate
        es_query["query"]["bool"]["filter"][0]["range"]["@timestamp"]["lt"] = self.edate
        return es_query
