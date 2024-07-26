from datetime import datetime, timedelta
from dags.config.logging import logging
from dags.config.constant import root_dir_v2
from dags.bos.bos_client_holder import BosClientHolder
# from dags.es.version2_dump import ESClientHolder
import glob
import os
from utilsByresId import ESClientHolder


def dump_es_data(ds, index):
    """

    :param ds:
    :param index:
    :param kwargs:
    :return:
    """
    logging.info(f'当前运行的北京时间日期:{ds}')
    dir = f'{root_dir_v2}{ds}'
    dir_analyse = f'{root_dir_v2}{ds}/analysis/'
    all_csv = glob.glob(os.path.join(dir, 'version_*fc*pk*.csv'))
    full_day_csv = dir_analyse + f'origin-my_index-{ds}.csv.zip'
    # 从bos中拉取当前日志,没办法设置查询条件的
    bos_exists = BosClientHolder(ds, root_dir=root_dir_v2).down_file(file_name=f'origin-my_index-{ds}.csv.zip')

    if not bos_exists or not os.path.exists(full_day_csv) or not all_csv:
        logging.info("bos中不存在原始文件:开始从ES中拉取")
        # es = ESClientHolder(ds)
        # start_date = es._split(file_count=4)
        # end_date = es._split(file_count=4)
        # es.dump_single(start_date, end_date)
    elif bos_exists:
        logging.info("bos中存在原始文件:不需要从ES中拉取")
    elif os.path.exists(full_day_csv):
        logging.info("存在全天完整数据,不需要从ES中拉取")
    else:
        logging.error("不存在全天完整数据，程序运行中断需要重新运行")