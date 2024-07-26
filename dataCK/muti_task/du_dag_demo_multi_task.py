# !/usr/bin/env python3
# -*- coding: utf-8 -*-
""" 获取多任务数据信息 """
import sys
import os
import re
import json
import pandas as pd
sys.path.append('/opt/airflow/')
from datetime import datetime, timedelta
from pprint import pprint

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from dags.config.logging import logging
from dags.es.multitask_dump import BaseClientHolder
from airflow.utils.dates import days_ago
from dags.notify.hi import Hi
from dags.config.constant import root_dir_mutitask

def failure_callback(context):
    dag = context['dag']
    taskIns = context['task_instance']
    Hi().send(f"日志 2.x DAG:{dag.dag_id} TASK:{taskIns.task_id} 持续时间:{'%.1f' % taskIns.duration}秒 运行失败")


def on_success_callback(context):
    dag = context['dag']
    taskIns = context['task_instance']
    if taskIns.task_id == 'store_bos_monitor':
        Hi().send(f"日志 2.x DAG:{dag.dag_id} TASK:{taskIns.task_id} 持续时间:{'%.1f' % taskIns.duration}秒 运行成功")


def on_retry_callback(context):
    dag = context['dag']
    taskIns = context['task_instance']
    Hi().send(f"日志 2.x DAG:{dag.dag_id} TASK:{taskIns.task_id} 持续时间:{'%.1f' % taskIns.duration}秒 运行重试")


args = {
    'owner': 'airflow',
    'start_date': days_ago(3),
    'depends_on_past': False,
    'retries': 1,
    'on_failure_callback': failure_callback,
    'on_success_callback': on_success_callback,
    'on_retry_callback': on_retry_callback,
    'retry_delay': timedelta(minutes=10),
}


def convert_CST(utc_date):
    dt_utc = datetime.strptime(utc_date[:19], "%Y-%m-%dT%H:%M:%S")
    local_date = (dt_utc + timedelta(hours=8)).strftime('%Y-%m-%d')
    return local_date


dag = DAG(
    dag_id='example_du_home_muti_task',
    default_args=args,
    schedule_interval='1 21 * * *',
    tags=['demo']
)

# [START howto_operator_python]
def print_context(**kwargs):
    pprint(kwargs)
    logging.info(f'当前运行的  UTC  日期  execution_date {kwargs["execution_date"]}')
    logging.info(f'当前运行的北京时间日期  execution_date {convert_CST(str(kwargs["execution_date"]))}')
    return 'Whatever you return gets printed in the logs'

run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

def getReData(msg, pattern=r'"logId":"(.*?)",', result=None):
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
        return result

def analysis(es_result, outputFilePath):
    count = 0
    resultData = []
    singleCount, mutiCount = 0, 0
    for line in es_result:
        count += 1
        # todo update
        # js = json.loads(line)
        msg = line["_source"]["message"]
        ts = msg.strip().split(" ", maxsplit=1)[0]
        botName = getReData(msg, r'"botName":"(.*?)"')
        taskNum = 0
        domain, intent = "", ""
        # todo:update
        if "sequence" in msg:
            mutiCount += 1
            taskType = "muti-task"
            # 获取results
            # todo:update
            msgJson = json.loads(msg[msg.find("{"):])
            extension = msgJson.get("dataana", {}).get("payload", {}).get("extension", {})
            if isinstance(extension, str):
                results = json.loads(extension).get("origin", {}).get("results", [])
                # todo update
                # domainList, intentList = [], []
                taskNum = len(results)
                for res in results:
                    domain += res.get("domain", "") + "&"
                    intent += res.get("intent", "") + "&"
                    # intentList.append(res.get("intent"))
                # for i in range(len(results)):
                #     domain += domainList[i]+"&"
                    # intent += intent[i]+ "&"
                domain = domain[:-1]
                intent = intent[:-1]
        else:
            taskType = "single-task"
        if taskType == "single-task":
            singleCount += 1
            #  提取ts"bot_id":"rock_sweep"\\\"domain\\\":\\\"roborock_clean_function\\\"
            domain = getReData(msg, r'\\"domain\\":\\"(.*?)\\"', "")
            intent = getReData(msg, r'\\"intent\\":\\"(.*?)\\"', "")
            taskNum = 1
        pk = getReData(msg, r'"pk":"(.*?)"')
        # domain、intent,taskNum,date判断是否为空
        if (domain.replace("&", "") != "" and intent.replace("&", "") != "" and taskNum!=0 and ts != "" and botName is not None and pk is not None):
            resultData.append([ts, botName, pk, domain, intent, taskType, taskNum])
    logging.info(f"总数据量：{count}, 单任务数据量:{singleCount}, 多任务数据量：{mutiCount}")
    dataTask = pd.DataFrame(resultData, columns=["ts", "botName", 'pk', "domain", "intent", "task_type", "task_num"])
    resultDf = dataTask.value_counts().reset_index()
    resultDf.columns = ['ts', 'botName','pk','domain', 'intent', 'task_type', 'task_num', 'count']
    resultDf.to_csv(outputFilePath, index=False)

def load_data(ds):
    """
    :param ds:
    :return:
    """
        # os.remove(file)
    # 直接拉取ES和转CSV,不保存
    es_result = BaseClientHolder(date=ds).dump_batch()
    return es_result
    
def load_and_ana(ds, **kwargs):
    file = root_dir_mutitask + 'muti_task_static.csv'
    ds = convert_CST(str(kwargs["execution_date"]))
    logging.info(f'当前运行的北京时间日期:{ds}')
    if not os.path.exists(root_dir_mutitask): os.makedirs(root_dir_mutitask)
    es_result = load_data(ds)
    analysis(es_result, file)
    

def save_table():
    from dags.db.mysql.badiu_util import db_engine, mapping_df_types
    data = pd.read_csv(root_dir_mutitask + 'muti_task_static.csv')
    data.columns = ['ts', 'botName','pk','domain', 'intent', 'task_type', 'task_num', 'count']
    engine = db_engine()
    dtypedict = mapping_df_types(data)
    data.to_sql('smarthome_query_task', engine, if_exists='append', index=False, dtype=dtypedict)

task1 = PythonOperator(
    task_id='load_and_ana',
    provide_context=True,
    python_callable=load_and_ana,
    dag=dag,
)

task2 = PythonOperator(
    task_id='save_table',
    provide_context=True,
    python_callable=save_table,
    dag=dag,
)

run_this >> task1 >> task2