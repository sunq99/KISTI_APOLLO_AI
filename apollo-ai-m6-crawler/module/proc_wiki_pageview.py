### WIKI PAGEVIEW 수집(4_4.WIKI PAGEVIEW CRAWL)
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
import module.data_connect as data_connect
import module.plus_func as pf
import module.logger as logger
import datetime

### WIKI PAGEVIEW 수집
def crawl_pageviews(year,seed):
    seed=seed.replace('/','%2F').replace('+','%2B').replace('?','%3F')
    url = f'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/user/{seed}/monthly/{year}0101/{year}1231'
    with requests.Session() as S:
        Max_retries = 10 
        S.mount("https://", HTTPAdapter(max_retries=Max_retries))
        user_agent = "GitLab CI automated test (/generated-data-platform/aqs/analytics-api) compare-page-metrics.py"
        crawl_data = S.get(url=url, headers={'User-Agent': user_agent}, verify=False, timeout=10)
        if crawl_data.status_code == 200:
            text_json = crawl_data.json()
            data_df = pd.DataFrame(text_json['items'])
        else:
            data_df=pd.DataFrame()
    return data_df

### WIKI PAGEVIEW 수집(item 테이블 기준)
def wiki_info_crawl(start_year,end_year,item_tb,config,progress_container):
    itemtb_len=len(item_tb)
    for index, row in item_tb.iterrows():#total=len(item_tb),desc="xtools process")
        try:
            seed = row["title"]
            id = row["id"]
            seed = seed[0].upper()+seed[1:] if len(seed)!=1 else seed.upper()
            year_cnt = end_year - start_year + 1
            for y in range(year_cnt):
                year = start_year + y
                xtool_connect= data_connect.Data_connect(["DB","mysql"],config,table_config=config['table']['CRAWL_XTOOL'])
                pageviews_df=crawl_pageviews(year,seed)
                if len(pageviews_df)>0:
                    pageviews_df.insert(0, 'id', id)
                    xtool_connect.write_data(pageviews_df,index=0)
                xtool_connect.close()   
            ratio = (index+1)/itemtb_len
            logger.progress_writer(progress_container,ratio,f"pageview 수집 중...({ratio*100:.1f}%)")       
        except Exception as ex:
            pf.log_error('error_log_xtool.txt',id, seed, ex)
    return None

### WIKI PAGEVIEW 수집(UI 연결)
def xtools_crawl(log_container,log_name,progress_container,conf_name,start_year,end_year,only_update):
    conf=pf.set_config(conf_name)
    if only_update=='n':
        #Xtools - new 생성
        xtool_dc= data_connect.Data_connect(["DB","mysql"],conf,table_config=conf['table']['CRAWL_XTOOL'])
        item_tb=xtool_dc.read_query_data("distinct id,title"," where id not in (select distinct id from {})".format(conf['table']['CRAWL_XTOOL']['target_table'][0]),index=0)
        xtool_dc.close()
        if len(item_tb)>0:
            logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : Pageview API로 수집해야할 아이템의 개수는 {len(item_tb)}")
            wiki_info_crawl(start_year,end_year,item_tb, conf,progress_container)
        else :
            logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : 새로 수집할 Item 없음")
    elif only_update=='y':
        #Xtools - update 생성
        xtool_dc= data_connect.Data_connect(["DB","mysql"],conf,table_config=conf['table']['CRAWL_XTOOL'])
        item_tb=xtool_dc.read_query_data("distinct id,title",f"where id in (select id from (select distinct id, max(REG_DATE) as max_year from {conf['table']['CRAWL_XTOOL']['target_table'][0]} group by id) as A where left(max_year,4)<{end_year})",index=0)
        new_startyear=int(xtool_dc.read_query_data("left(max(REG_DATE),4) as year","",index=1)['year'][0])
        xtool_dc.close()
        if len(item_tb)>0:
            logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : [update] xtools로 수집해야할 아이템의 개수는 {len(item_tb)} -> 수집 기간 : {str(new_startyear)} ~ {str(end_year)}")
            wiki_info_crawl(new_startyear,end_year,item_tb,conf,progress_container)
        else :
            logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : 업데이트 수집할 Item 없음")
    else : 
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : option을 재확인해주세요.")
    return 1

