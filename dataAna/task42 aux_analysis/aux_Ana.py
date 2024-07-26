"""入口函数:分析aux的技能分布和激活地区分布"""

import os
import re
import glob
import pandas as pd
import numpy as np
import requests
import json
from dags.temp.base_temp_es_client_holder import ESClientHolder, file_list
import math
from datetime import datetime, timedelta
from dags.config.logging import logging
from dags.config.constant import root_dir_v2
from dags.bos.bos_client_holder import BosClientHolder
from visual_plotMap import *
from pyecharts.charts import Tab

def dump_bos_data(sdate, edate):
    """

    :param ds: 时间
    :param index:
    :param kwargs:
    :return:
    """
    date_list = getDateList(sdate=sdate, edate=edate)
    print(date_list)
    for sdate in date_list:
        logging.info(f'当前运行的北京时间日期:{sdate}')
        dir = f'{root_dir_v2}{sdate}'
        dir_analyse = f'{root_dir_v2}{sdate}/analysis/'
        all_csv = glob.glob(os.path.join(dir, 'version_*fc*pk*.csv'))
        full_day_csv = dir_analyse + f'origin-my_index-{sdate}.csv.zip'
        # 从bos中拉取当前日志,没办法设置查询条件的
        bos_exists = BosClientHolder(sdate, root_dir=root_dir_v2).down_file(file_name=f'origin-my_index-{sdate}.csv.zip')
        if not bos_exists or not os.path.exists(full_day_csv) or not all_csv:
            logging.info("bos中不存在原始文件:开始从ES中拉取")
        elif bos_exists:
            logging.info("bos中存在原始文件:不需要从ES中拉取")
        elif os.path.exists(full_day_csv):
            logging.info("存在全天完整数据,不需要从ES中拉取")
        else:
            logging.error("不存在全天完整数据，程序运行中断需要重新运行")


def dump_version2(sdate, edate, savePath, days=1):
    """
    拉取数据
    Args:
        date (_type_): 拉取数据的日期
    """
    es_client = ESClientHolder(sdate=sdate,edate=edate, savePath=savePath)
    es_client.dump_batch()


def getDateList(sdate, edate):
    sdate = datetime.strptime(sdate, "%Y-%m-%d")
    edate = datetime.strptime(edate, "%Y-%m-%d")
    date_list = []
    while sdate <= edate:
        date_list.append(sdate.strftime("%Y-%m-%d"))
        sdate += timedelta(days=1)
    return date_list

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



import time
def get_address_ByIp(ip, url = "https://qifu.baidu.com/ip/geo/v1/district"):
        print(ip)
        param = {'ip':ip,
                'json':'true',
                
                } 
        ree = requests.get(url, params = param)
        try:
                re = json.loads(ree.text.replace("\\"," "))
                re = re.get("data")
        except Exception as e:
                print(f"json解析出错,网站返回结果为:{ree.text}")
                return None, None
        print(re)
        if not isinstance(re, dict):
                return None, None
        time.sleep(0.5)
        return (re.get("prov"), re.get("city"))
    

def get_address_ByIp2(ip, url= 'http://whois.pconline.com.cn/ipJson.jsp'):
        print(ip)
        param = {'ip':ip,
                'json':'true',
                
                } 
        ree = requests.get(url, params = param)
        try:
                re = json.loads(ree.text.replace("\\"," "))
        except Exception as e:
                print(f"json解析出错,网站返回结果为:{ree.text}")
                return None, None
        print(re)
        if not isinstance(re, dict):
                return None, None
        time.sleep(0.5)
        return (re.get("pro"), re.get("city"))

# def read_table():
#     from dags.db.mysql.badiu_util import db_connect
#     sql = 'select fc, pk, ak, state, client_ip, active_time from smarthome_voice_device where client_ip != "" and pk = "15k4eg6e"'
#     domain_distribute = pd.read_sql(sql, db_connect())
#     # 将ip转换为地址
#     domain_distribute['pro'], domain_distribute['city'] = zip(*domain_distribute["client_ip"].apply(get_address_ByIp))
#     return domain_distribute

def read_table(datapath):
    '''datapath = "dags/temp/task42 aux_analysis/test.csv"'''
    domain_distribute = pd.read_csv(datapath)
    domain_distribute['pro'], domain_distribute['city'] = zip(*domain_distribute["client_ip"].apply(get_address_ByIp))
    for client_ip in domain_distribute.loc[(domain_distribute["city"] == "") | (domain_distribute["pro"] == ""), "client_ip"]:
        pro, city = get_address_ByIp2(client_ip)
        if pro is not None and pro != "":
            domain_distribute.loc[domain_distribute["client_ip"]==client_ip, "pro"] = pro
        if city is not None and city != "":
            domain_distribute.loc[domain_distribute["client_ip"]==client_ip, "city"] = city
    domain_distribute.loc[(domain_distribute["city"] == "") | (domain_distribute["pro"] == ""), :]
    return domain_distribute

def save_to_sql(domain_distribute):
    pass

def visual_polt(domain_distribute):
    '''绘制可视化大屏'''
    # ipDataDf = pd.read_csv("./data.csv", sep='\t')
    # ipMapDf = pd.read_csv("./ipMap.csv")
    # dataDf = pd.merge(ipDataDf, ipMapDf, on="client_ip", how="left")
    domain_distribute.drop_duplicates(subset=["fc", "pk", "ak", "client_ip"], inplace=True)
    domain_distribute.head()
    # 将多个图表对象汇总到一个图中
    cityDf = domain_distribute["city"].value_counts().reset_index()
    cityChart = plotCityMap(cityDf)
    
    temp = domain_distribute["date"].value_counts().sort_index(ascending=True)
    full_date_range = pd.date_range(start="2023-12-19", end="2024-07-01")
    # 重新索引 DataFrame，并填充缺失的日期数据为零
    line_data = temp.reindex(full_date_range, fill_value=0)
    
    proDf = domain_distribute["pro"].value_counts()
    proChart = map_chart(proDf.index.to_list(), proDf.to_list())
    proPieChart = plotPie(proDf.reset_index().values.tolist(), "省份占比饼图")
    pieCityData = cityDf.head(8)
    pieCityData = pd.concat([pieCityData,pd.DataFrame({'city': ['其他'], 'count': [cityDf[8:]["count"].sum()]})], axis=0, ignore_index=True)
    cityPie = plotPie(pieCityData.values.tolist(), "城市占比")
    auxByDayLine = line_plot(line_data.index.to_list(), line_data.values.tolist(), "日首次激活数", "奥克斯日激活数")
    tab = Tab()
    tab.add(proChart, "奥克斯省份分布地图数据")
    tab.add(proPieChart, "奥克斯省份分布占比数据")
    tab.add(cityChart, "奥克斯城市分布地图数据")
    tab.add(cityPie, "奥克斯城市分布占比数据")
    tab.add(auxByDayLine, "奥克斯每日首次激活量")
    tab.render("aux_day_visualMap.html")


def data_day7(data_dir):
    "receive unit service response", ""
    """
    拉取数据
    Args:
        date (_type_): 拉取数据的日期
    """
    files = sorted(file_list(data_dir, filetype=".json"))
    count= 0
    reqIds = []
    for index, file in enumerate(files):
        with open(file, 'r') as f:
            while True:
                line = f.readline()
                if line:
                    count+=1
                    js = json.loads(line)
                    msg = js["_source"]["message"]
                    ts = msg.split("  ")[0]
                    reqId = getReData(msg, pattern=r'  : \[(.*?)\]')
                    logId = getReData(msg, pattern=r'"logId":"(.*?)"')
                    ak = getReData(msg, pattern=r'"ak":"(.*?)"')
                    fc = getReData(msg, pattern=r'"fc":"(.*?)"')
                    pk = getReData(msg, pattern=r'"pk":"(.*?)"')
                    version = getReData(msg, pattern=r'"version":"(.*?)"')
                    query = getReData(msg , r'"query":"(.*?)"')
                    domain = getReData(msg , r'\\\"domain\\\":\\\"(.*?)\\\"')
                    intent = getReData(msg , r'\\\"intent\\\":\\\"(.*?)\\\"')
                    # content = getReData(msg, r'"content":"(.*?)"')
                    botName = getReData(msg, r'"botName":"(.*?)"')
                    if ak is not None and fc is not None and pk is not None and query is not None:
                        reqIds.append([ts, reqId, logId, ak, fc, pk, version, query, domain, intent, botName])
                else:
                    break
    print(f"count:{count}")
    df = pd.DataFrame(reqIds, columns=['ts', 'reqId', 'logId', 'ak', 'fc', 'pk', 'version', 'query', 'domain', 'intent', 'botName'])
    df = df[df['pk']=="15k4eg6e"]
    df.drop_duplicates(inplace=True)
    df.to_csv("data_aux_7days.csv", index=False)


# 计算指标
if __name__ == "__main__":
    sdate = "2024-06-24 00:00:00"
    edate = "2024-07-01 00:00:00"
    savePath = "data/task42"
    # dump_version2(sdate=sdate, edate=edate, savePath=savePath)
    dump_bos_data(sdate="2024-06-01", edate="2024-06-24")
    data_day7(savePath)
    # savePath = f"data/version2_0/{sdate}/analysis/origin-my_index-{sdate}.csv.zip"
    '''
    '''
    # for date in dateList:
    #     getBosTime(data_dir=f"data/version2_0/{date}/analysis/origin-my_index-{date}.csv.zip", date=date, savePath= savePath)
    # getBosTime(data_dir="data/version2/analysis",sdate=sdate, edate=edate)
