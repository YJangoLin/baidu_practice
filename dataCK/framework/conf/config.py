
# 数据存储的根路径
DATA_ROOR_DIR = "data/version"
SAVE_PATH = "data/output"

# 查询语句的json文件
STARTDATE = "2024-6-02"
ENDDATE = "2024-6-03"
QUERYCOLUMNS = ["message"] # 查询的字段
QUERYFILTERS = {
    "must": [
        {"match": {"message": "ze0mpx"}},
        {"match": {"message": "ASR2IOT"}}
        ] # 额外的添加过滤条件
                }
es_query_dict = {
        "_source": QUERYCOLUMNS,
        "query": {
            "bool": {
                "filter": [
                    {
                        "range": {
                        "@timestamp": {
                            "gte": "",
                            "lt": "",
                            "format": "yyyy-MM-dd HH:mm:ss",
                            "time_zone": "+08:00",
                        }
                        }
                    }
                ]
            }
        },
    }
es_query_dict["query"]["bool"].update(QUERYFILTERS)
IS_DYNAMIC_QUERY = False # 是否动态查询
QUERY_CLUSTER = "shc-v1*" # 查询的集群，注意“_”和“—”

# 正则匹配, 默认提取出ts，reqId
''' other re: 
        elapse_time: r"elapse_time=([-+]?[0-9]*.?[0-9]+)",  # 这些都是shc-model*中常用的统计指标
        type and request: r"\[\] (.*?) skill (.*?) \= \{"
        "log_id": r"INFO - log_id=(.*?),",   
        "domain": r"'domain': '(.*?)',",
        "elapse_time": r"elapse_time=([-+]?[0-9]*.?[0-9]+)",
'''
PATTEN = {
    "reqId": r": \[(.*?)\]\[\]",
    "logId": r'"logId":"(.*?)"',
    "seg": r'"seg":"(.*?)"',
    "domain": r'\\"domain\\":\\"(.*?)\\"'
}

# 判断是否包含
STRCONTRAINCHECK = {
    "高频query缓存命中": True, # 统计包含缓存的reqId,
    "必过集缓存命中": True, # 统计包含缓存的reqId
    "get highest priority skill service is XiaoDuModelService": False, # 统计不包含缓存的reqId
}


# 拆分为几个时间段--代表使用几个线程，最好在4个线程，一天天的拉取数据
WORK_NUM = 4