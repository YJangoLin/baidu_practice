from analysis import Analysis
from utils.util import get_file, download
from conf.config import DATA_ROOR_DIR,STARTDATE, ENDDATE, SAVE_PATH



if __name__ == '__main__':
    fileDir = DATA_ROOR_DIR
    filePath = get_file(fileDir, ".csv")
    download(sdate=STARTDATE, endDate=ENDDATE, savePath=SAVE_PATH)
    # ana = Analysis(filePath)
    # ana.analysis()
