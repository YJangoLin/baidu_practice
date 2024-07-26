from DrissionPage import ChromiumPage
from DrissionPage import ChromiumOptions
from config import url, pageList
import pandas as pd
import numpy as np
import time
# driverPath = ""
# ChromiumOptions().set_browser_path(driverPath).save()


def getPage(base_url, page, PageNum=None):
    if PageNum is None:
        cur_url = base_url
    else:
        cur_url = base_url + f"/{PageNum}/"
    print(f"visit web: {cur_url}")
    page.get(cur_url)
    return page

def parse_page(page, dataList):
    try:
        count = 0
        bookListEle = page.ele('xpath://div[@class="albums _ZV"]')
        liEles = bookListEle.eles('xpath://li//a[@class="album-title line-2 lg bold T_G"]')
        for li in liEles:
            count += 1
            title = li.attrs.get('title')
            href = li.attrs.get('href')
            dataList.append([title, href])
        print(f"dataNum: {count}")
    except Exception as e:
        return dataList, count
    return dataList, count
    

def analysis(dataDf):
    # dataDf = pd.DataFrame(dataList, columns=["title", "href"])
    dataDf["title"] = dataDf["title"].apply(lambda x: x.split("丨")[0].split("|")[0].split("｜")[0])
    dataDf.drop_duplicates(inplace=True)
    dataDf.to_excel("spider_voice_book.xlsx", index=False)

if __name__ == '__main__':
    dataDf = pd.read_excel("spider_voice_book.xlsx")
    analysis(dataDf)
    
    
