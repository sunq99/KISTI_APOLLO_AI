### 지표 및 검색 테이블 처리(5_5.WIKI MAKETABLE)
import networkx as nx
from sklearn.preprocessing import MinMaxScaler
import module.data_connect as data_connect
import module.plus_func as pf
import pandas as pd
import module.logger as logger
import time
import datetime

### PAGERANK 테이블 생성(UI 연결)
def make_pagerank(conf_name,log_container,log_name):
    conf=pf.set_config(conf_name)
    table_config=conf['table']['MAKE_PAGERANK']
    kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
    try:
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : Network 테이블 읽기")
        key_df = kisti_connect.read_data(0)
        key_df = key_df.drop_duplicates().reset_index(drop=True)
        scaler = MinMaxScaler()
        g = nx.from_pandas_edgelist(key_df, 'FROM_ID', 'TO_ID')
        w_dict = nx.pagerank(g,max_iter=200)
        page_rank = pd.DataFrame(w_dict, index=['pagerank']).T.reset_index()
        page_rank['index']=page_rank['index'].astype('int')
        scaler = MinMaxScaler(feature_range=(1,100))
        page_rank['pagerank']=scaler.fit_transform(page_rank[['pagerank']])
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : PageRank 테이블 생성")
        kisti_connect.delete_record(index=0)
        kisti_connect.write_data(page_rank,index=0)
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : PageRank 테이블 적재")
    finally:
        kisti_connect.close()

### 연도별 테이블 생성(UI 연결)
def make_statistics(conf_name,log_container,log_name):
    conf=pf.set_config(conf_name)
    scaler = MinMaxScaler(feature_range=(0,100))
    try:
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : 연도별 지표 테이블 시작")
        start_time=time.time()
        statics_dc= data_connect.Data_connect(["DB","mysql"],conf,table_config=conf['table']['MAKE_STATICS'])
        #make_edits
        item_tb=statics_dc.read_data(0)
        edit_tb=statics_dc.read_data(1)
        edit_tb=edit_tb[['ID','YEAR','EDITS']]
        if len(edit_tb)>0:
            edit_tb['YEAR']=edit_tb['YEAR'].astype(int)
            max_year = edit_tb['YEAR'].max()
            logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : 연도별 지표 테이블 생성 : 2015 ~ {max_year}")
        else:
            logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : edits 테이블 값이 존재하지 않습니다.")
            return 0
        edit_tb=edit_tb[['ID','YEAR','EDITS']].drop_duplicates()
        edit_tb.sort_values(by=['ID', 'YEAR'], inplace=True)
        edit_tb['Cumulative_Edits'] = edit_tb.groupby(['ID'])['EDITS'].cumsum()
        edit_tb['EPV'] = edit_tb['EDITS'] / edit_tb['Cumulative_Edits']
        all_years = pd.DataFrame({'YEAR': list(range(2015, max_year+1))})
        all_ids = item_tb['ID'].unique()
        all_combinations = pd.MultiIndex.from_product([all_ids, all_years['YEAR']], names=['ID', 'YEAR']).to_frame(index=False)
        edit_tb = pd.merge(all_combinations, edit_tb, on=['ID', 'YEAR'], how='left')
        edit_tb['EPV'] = edit_tb['EPV'].fillna(0).astype(float)  # 결측치는 0으로 대체
        read_edit_time=time.time()
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : Read&Truncate time(edit)-{read_edit_time-start_time}")
        #make_pageviews
        pageview_tb=statics_dc.read_data(2).drop_duplicates()
        pageview_tb['YEAR'] = pd.to_datetime(pageview_tb['REG_DATE'], format='%Y%m%d00').dt.year
        pageview_tb=pageview_tb[['ID','YEAR','VIEWS']]
        pageview_tb['YEAR'] = pageview_tb['YEAR'].astype(int)
        pageview_tb['Year_Views'] = pageview_tb.groupby(['ID', 'YEAR'])['VIEWS'].transform('sum')
        pageview_tb=pageview_tb[['ID','YEAR','Year_Views']].drop_duplicates()
        pageview_tb.sort_values(by=['ID', 'YEAR'], inplace=True)
        pageview_tb['Cumulative_Views'] = pageview_tb.groupby(['ID'])['Year_Views'].cumsum()
        pageview_tb['PAGEVIEWS'] = pageview_tb['Year_Views'] / pageview_tb['Cumulative_Views']
        read_view_time=time.time()
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : Read&Truncate time-{read_view_time-read_edit_time}")
        total_df=pd.merge(edit_tb[['ID', 'YEAR', 'EPV']],pageview_tb[['ID', 'YEAR', 'PAGEVIEWS']],on=["ID","YEAR"],how='left')
        total_df['NORM_PAGEVIEWS']=scaler.fit_transform(total_df[['PAGEVIEWS']])
        total_df['NORM_EPV']=scaler.fit_transform(total_df[['EPV']])
        norm_time=time.time()
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : Norm tim-{norm_time-read_view_time}")
        total_df=total_df[['ID','YEAR','PAGEVIEWS','EPV','NORM_PAGEVIEWS','NORM_EPV']]
        total_df.fillna(0,inplace=True)
        statics_dc.delete_record(index=0)
        statics_dc.write_data(total_df,index=0)
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : write time-{time.time()-norm_time}")
    except Exception as e:
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : 처리 중 오류 발생 - {e}")
    finally:
        statics_dc.close()
    return 1

### itemlist 테이블 생성(UI 연결)
def make_itemlist(conf_name,log_container,log_name):
    conf=pf.set_config(conf_name)
    table_config=conf['table']['MAKE_WIKITEMLIST']
    kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
    logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : item list 테이블 시작")
    try:
        start_time=time.time()
        item_df=kisti_connect.read_data(index=0)
        pagerank_df=kisti_connect.read_data(index=1)
        stats_df=kisti_connect.read_data(index=2)
        if len(stats_df)>0:
            stats_df['BASE_YEAR']=stats_df['BASE_YEAR'].astype(int)
            max_year =stats_df['BASE_YEAR'].max()
            logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : item list 테이블 생성 - {max_year}")
        else:
            logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : 지표 테이블 값이 존재하지 않습니다.")
            return 0
        stats_df=stats_df[stats_df["BASE_YEAR"]==int(max_year)][['ID','NORM_PAGEVIEWS','NORM_EPV']].drop_duplicates()
        pagerank_df.columns=['ID','pagerank']
        stats_df.columns=['ID','pageviews','EPV']
        current_stat_df= pd.merge(item_df[['ID','TITLE']], pagerank_df, on='ID', how='left').drop_duplicates()
        current_stat_df= pd.merge(current_stat_df, stats_df, on='ID', how='left')
        current_stat_df['pagerank'] = current_stat_df['pagerank'].fillna(0).astype(float)  # 결측치는 0으로 대체
        current_stat_df['pagerank']=current_stat_df['pagerank'].astype(float)
        current_stat_df['pageviews'] = current_stat_df['pageviews'].fillna(0).astype(float)  # 결측치는 0으로 대체
        current_stat_df['pageviews']=current_stat_df['pageviews'].astype(float)
        current_stat_df['EPV'] = current_stat_df['EPV'].fillna(0).astype(float)  # 결측치는 0으로 대체
        current_stat_df['EPV']=current_stat_df['EPV'].astype(float)
        kisti_connect.delete_record(index=0)
        kisti_connect.write_data(current_stat_df,index=0)
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : write time-{time.time()-start_time}")
    except Exception as e:
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : 처리 중 오류 발생 - {e}")
    finally:
        kisti_connect.close()
        
### search 테이블 생성(UI 연결)
def make_search_tb(conf_name,log_container,log_name):
    conf=pf.set_config(conf_name)
    table_config=conf['table']['MAKE_SEARCHTB']
    start_time=time.time()
    try:
        kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
        item_tb=kisti_connect.read_data(0)
        time_item_list=time.time()
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : 최종 item list 생성-{time_item_list-start_time}")
        item_list=list(item_tb["TITLE"])
        #item_list 적재
        item_tb["REDIRECT"]=item_tb["TITLE"]
        item_tb["TYPE"]="ITEM"
        item_tb["TECH_RANK"]=0
        item_tb["TECH_CNT"]=0
        kisti_connect.delete_record(0) # 기존 적재된 내용 삭제
        kisti_connect.write_data(item_tb,0)
        time_item_write=time.time()
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : search table item 적재-{time_item_write-time_item_list}")
        for i in range(0,len(item_list),10000):
            item_set=["\\\\".join(_.split("\\")) for _ in item_list[i:i+10000]]
            item_set=["\'\'".join(_.split("'")) for _ in item_set]
            str_item="','".join(item_set)
            print(item_tb)
            redirect_temp=kisti_connect.read_query_data(f"REDIRECT as TITLE,TITLE as REDIRECT","where REDIRECT in ('{}')".format(str_item),index=1)
            print(redirect_temp)
            if len(redirect_temp)>0:
                redirect_tb = pd.merge(item_tb[["ID","TITLE"]], redirect_temp, left_on='TITLE', right_on='TITLE', how='left')
                print(redirect_temp)
                redirect_tb["TYPE"]="REDIRECT"
                redirect_tb["TECH_RANK"]=0
                redirect_tb["TECH_CNT"]=0
                redirect_tb=redirect_tb.dropna(axis=0)
                kisti_connect.write_data(redirect_tb,0)
            else:
                logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} - Redirect 정보가 없습니다.")
        time_redirect_write=time.time()
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : search table redirect 적재-{time_redirect_write-time_item_list}")
    except Exception as e:
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : 처리 중 오류 발생-{e}")
    finally:
        kisti_connect.close()