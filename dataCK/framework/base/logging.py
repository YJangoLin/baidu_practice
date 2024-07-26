import logging

logging.basicConfig(
    format='%(asctime)s < pid:%(process)d >-< tid:%(threadName)s >- %(filename)s-[fun:%(funcName)s-line:%(lineno)d] - %(name)s - %(levelname)s: %(message)s',
    level=logging.INFO)
