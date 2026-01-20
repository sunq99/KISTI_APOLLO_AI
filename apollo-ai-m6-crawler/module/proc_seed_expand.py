### 아이템 테이블 생성(3_3.MAKE ITEM LIST)
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import pandas as pd
import numpy as np
import time,datetime
import re
import module.data_connect as data_connect
import module.plus_func as pf
import module.logger as logger

def check_item(conf,item_set,search_option=True): #WIKI DUMP로 입력 받은 Seeds의 TRUE TITLE 반환
    where_option= ' Binary' if search_option==True else ''
    item_set=[re.sub(r"\s"," ",str(_)) for _ in item_set]
    item_set=[_[0].upper()+_[1:] if len(_)>1 else _ for _ in item_set]
    if search_option==True:
        item_df = pd.DataFrame({"title":item_set})
    else:
        item_lower = [_.lower() for _ in item_set]
        item_df = pd.DataFrame({"title":item_set,"title_join":item_lower})
    item_set=["\\\\".join(_.split("\\"))  for _ in item_set]
    item_set=["\'\'".join(_.split("'")) for _ in item_set]
    str_item="','".join(item_set)
    dump_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=conf['table']['CRAWL_CHECKSEEDS'])
    item_tb=dump_connect.read_query_data("id,title","where{} title in ('{}')".format(where_option,str_item),index=0)
    if len(item_tb)==0:
        item_tb=pd.DataFrame(columns=["title","id"])
    result_item=["\\\\".join(_.split("\\")) for _ in list(item_tb['title'])]
    result_item=["\'\'".join(_.split("'")) for _ in result_item]
    list_set_redirect=list(set(item_set)-set(result_item)) #완전 일치시 제외
    item_tb["true_title"]=item_tb["title"]
    item_tb=item_tb[["title","id","true_title"]]
    if len(item_tb)==0:
        result_tb=pd.DataFrame(columns=["title","id","true_title"])
        return result_tb
    if len(list_set_redirect)>0 and search_option==True:
        str_item_redirect="','".join(list_set_redirect)
        redirect_tb=dump_connect.read_query_data("distinct title,redirect as true_title","where{} title in ('{}')".format(where_option,str_item_redirect),index=2)
        if len(redirect_tb)>0:
            redirect_set=["\\\\".join(_.split("\\")) for _ in list(redirect_tb['true_title'])]
            redirect_set=["\'\'".join(_.split("'")) for _ in redirect_set]
            str_redirct="','".join(redirect_set)
            item_tb_redirect=dump_connect.read_query_data("id,title as true_title","where{} title in ('{}')".format(where_option,str_redirct),index=0)
            if len(item_tb_redirect)==0 or len(redirect_tb)==0:
                result_tb=pd.DataFrame(columns=["title","id","true_title"])
                return result_tb
            df_join = pd.merge(redirect_tb, item_tb_redirect, left_on='true_title', right_on='true_title', how='left')
            if len(df_join)>0:
                redirect_join=df_join[["title","id","true_title"]]
                if len(item_tb)>0:
                    item_tb = pd.concat([item_tb,redirect_join])
                else : item_tb=redirect_join
    dump_connect.close()
    if search_option==True:
        item_df["title"]=item_df["title"].astype(str)
        item_tb["title"]=item_tb["title"].astype(str) 
        result_tb = pd.merge(item_df, item_tb, left_on='title', right_on='title', how='left')
    else :
        item_tb["title_join"]=item_tb.title.str.lower()
        item_tb=item_tb[["title_join","id","true_title"]]
        result_tb = pd.merge(item_df, item_tb, left_on='title_join', right_on='title_join', how='left')
        result_tb = result_tb[["title","id","true_title"]]
    result_tb=result_tb.replace({np.nan: None})
    return result_tb

#### SEED TABLE 적재(UI 연결)
def get_check_seed(conf_name,item_set):
    conf=pf.set_config(conf_name)
    if isinstance(item_set, str):
        dataset = [item_set]
    if isinstance(item_set, pd.DataFrame):
        dataset = list(item_set['Title'])
    if isinstance(item_set, list):
        dataset = item_set
    for ind in range(0,len(item_set),1000):
        result_temp = check_item(conf,dataset[ind:ind+1000])
        result = result_temp[result_temp["id"].notnull()]
        result_null = result_temp[result_temp["id"].isnull()]
        if len(result_null)>0:
            result_two = check_item(conf,result_null["title"],False)
            result = pd.concat([result,result_two])
        kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=conf['table']['CRAWL_CHECKSEEDS'])
        kisti_connect.write_data(result,index=0)
        kisti_connect.close()

# SEEALSO TABLE 생성(확장 시 FROM_TO ID 생성)
def n_expand(conf,item_tb,n_cnt):
    table_config=conf['table']['CRAWL_SEEALSO']
    if n_cnt==1:
        item_temp=item_tb["id"]
        str_id=','.join(str(s) for s in item_temp)
        kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
        seed_tb=kisti_connect.read_query_data("distinct id, title as from_title","where id in ({})".format(str_id),index=4)
        kisti_connect.close()
        seed_tb["from_id"]=seed_tb["id"]
        str_fromid=','.join(str(s) for s in seed_tb["id"])
        dump_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
        seealso_tb=dump_connect.read_query_data("id as from_id,seealso as to_temp","where id in ({})".format(str_fromid),index=3)
        dump_connect.close()
        if len(seealso_tb)>0:
            seealso_tb["to_temp"]=seealso_tb.to_temp.str.split("#").str[0]
            dataset=list(seealso_tb["to_temp"])
            seealso_tb = pd.merge(seed_tb, seealso_tb, left_on='from_id', right_on='from_id', how='left')
            to_result=pd.DataFrame(columns=["to_temp","to_id","to_title"])
            for ind in range(0,len(seealso_tb),10000):
                temp_to=check_item(conf,dataset[ind:ind+10000])
                temp_to.rename(columns={"title":"to_temp","id":"to_id","true_title":"to_title"},inplace=True)
                to_result=pd.concat([to_result,temp_to],axis=0)
            result = pd.merge(seealso_tb, to_result, left_on='to_temp', right_on='to_temp', how='left')
            result = result[result["to_id"].notnull()]
            result["n_cnt"]=n_cnt
            result=result[["id","from_id","from_title","to_id","to_title","n_cnt"]]
            result=result.drop_duplicates()
            result.reset_index()
            for ind in range(0,len(result),10000):
                write_df=result.loc[ind:ind+10000]
                write_df=write_df.replace({np.nan: None})
                kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
                kisti_connect.write_data(write_df,index=0)
                kisti_connect.close()
    elif n_cnt>1:
        item_temp=item_tb["id"]
        str_id=','.join(str(s) for s in item_temp)
        kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
        seed_tb=kisti_connect.read_query_data("distinct id, to_id as from_id, to_title as from_title","where id in ({}) and n_cnt={}".format(str_id,n_cnt-1),index=5)
        kisti_connect.close()
        if len(seed_tb)==0:
            return 0
        str_fromid=','.join(str(s) for s in seed_tb["from_id"])
        dump_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
        seealso_tb=dump_connect.read_query_data("id as from_id,seealso as to_temp","where id in ({})".format(str_fromid),index=3)
        dump_connect.close()
        if len(seealso_tb)>0:
            seealso_tb["to_temp"]=seealso_tb.to_temp.str.split("#").str[0]
            dataset=list(seealso_tb["to_temp"])
            seealso_tb = pd.merge(seed_tb, seealso_tb, left_on='from_id', right_on='from_id', how='left')
            to_result=pd.DataFrame(columns=["to_temp","to_id","to_title"])
            for ind in range(0,len(seealso_tb),10000):
                temp_to=check_item(conf,dataset[ind:ind+10000])
                temp_to.rename(columns={"title":"to_temp","id":"to_id","true_title":"to_title"},inplace=True)
                to_result=pd.concat([to_result,temp_to],axis=0)
            result = pd.merge(seealso_tb, to_result, left_on='to_temp', right_on='to_temp', how='left')
            result = result[result["to_id"].notnull()]
            result["n_cnt"]=n_cnt
            result=result[["id","from_id","from_title","to_id","to_title","n_cnt"]]
            result=result.drop_duplicates()
            mainid_list=list(set(list(result["id"])))
            for m_i in mainid_list:
                result_id=result[result["id"]==m_i]
                kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
                filter_tb=kisti_connect.read_query_data("distinct to_id","where id in ({}) and n_cnt<{}".format(m_i,n_cnt),index=5)
                kisti_connect.close()
                filter_list=list(set(list(filter_tb["to_id"])+[m_i]))
                result_id=result_id[~result_id["to_id"].isin(filter_list)]
                result_id.reset_index(drop=True, inplace=True)
                for ind in range(0,len(result_id),10000):
                    write_df=result_id.loc[ind:ind+10000]
                    write_df=write_df.replace({np.nan: None})
                    kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
                    kisti_connect.write_data(write_df,index=0)
                    kisti_connect.close()

#### SEEALSO TABLE 생성 및 적재(UI 연결)
def seealso_expand(log_container,log_name,progress_container,conf_name,n_cnt):
    conf=pf.set_config(conf_name)
    table_config=conf['table']['CRAWL_SEEALSO']
    kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
    where=f"where id not in (select distinct id from {table_config["source_table"][5]} where n_cnt={n_cnt}) and id is not null"
    item_tb=kisti_connect.read_query_data("distinct id,true_title as title",where,index=4)
    kisti_connect.close()
    if len(item_tb)==0:
        logger.log_writer(log_container,log_name,f'{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : 이미 확장한 차수이므로 다음 차수로 넘어갑니다.')
        return 0
    start_time=time.time()
    logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : {n_cnt}차 확장 시작")
    if n_cnt==1:
        n_expand(conf,item_tb,1)
    elif n_cnt>1:
        item_tb_len=len(item_tb)
        for ind in range(0,len(item_tb),1000):
            n_expand(conf,item_tb.loc[ind:ind+1000],n_cnt)
            ratio = ind/(item_tb_len-1)
            logger.progress_writer(progress_container,ratio,f"{n_cnt}차 확장 중...({ratio*100:.1f}%)")
    finish_time=time.time()
    logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : {n_cnt}차 확장 완료 : 소요시간 {finish_time-start_time}")
    return 1

#### ITEM_MASTER TABLE 적재(RULE 적용)(UI 연결 모듈)
def filter_item(log_container,log_name,progress_container,conf_name): 
    conf=pf.set_config(conf_name)
    table_config=conf['table']['CRAWL_SEEALSO_FILTER']
    logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : wiki Rule 불러오기 시작")
    wiki_rule=pd.read_excel(table_config["rule_dir"])
    cate_rule=list(wiki_rule[wiki_rule['col']=='category']['item'])
    cate_rules=('|').join(cate_rule)
    title_rule=list(wiki_rule[wiki_rule['col']=='title']['item'])
    title_rules=('|').join(title_rule)
    c_rule=re.compile('(?<![A-Z])('+cate_rules.upper()+')(?![A-Z])')
    t_rule=re.compile('(?<![A-Z])('+title_rules.upper()+')(?![A-Z])')
    logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : wiki Rule 불러오기 완료")
    kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
    seed_tb=kisti_connect.read_query_data("distinct id","where id is not null",index=1)
    seed_tb=list(seed_tb["id"])
    kisti_connect.close()
    seedtb_len=len(seed_tb)
    for ind in range(0,len(seed_tb),1000):
        seed_temp=seed_tb[ind:ind+1000]
        str_id=','.join(str(s) for s in seed_temp)
        kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
        filter_source=table_config["source_table"][3]
        to_tb=kisti_connect.read_query_data("distinct TO_ID as id,TO_TITLE as title","where ID in ({}) and TO_ID not in (Select distinct id from {})".format(str_id,filter_source),index=2)
        from_tb=kisti_connect.read_query_data("distinct FROM_ID as id,FROM_TITLE as title","where ID in ({}) and FROM_ID not in (Select distinct id from {})".format(str_id,filter_source),index=2)
        kisti_connect.close()
        item_tb=pd.concat([to_tb,from_tb])
        item_tb=item_tb.drop_duplicates()
        item_tb.reset_index(drop=True, inplace=True)
        for ind_item in range(0,len(item_tb),10000):
            item_temp=item_tb.loc[ind_item:ind_item+10000]
            str_item=','.join(str(s) for s in list(item_temp["id"]))
            dump_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
            category_tb=dump_connect.read_query_data("ID as id,GROUP_CONCAT(distinct CATEGORY SEPARATOR '|') as category","where ID in ({}) group by ID".format(str_item),index=0)
            dump_connect.close()
            cateogory_item = pd.merge(item_temp, category_tb, left_on='id', right_on='id', how='left')
            cateogory_item["title_filter"] = cateogory_item["title"].apply(lambda x: t_rule.search(x.upper()))
            cateogory_item["category_filter"] = cateogory_item["category"].apply(lambda x: c_rule.search(x.upper()))
            write_df=cateogory_item[cateogory_item["title_filter"].isnull()&cateogory_item["category_filter"].isnull()][["id","title"]]
            kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
            kisti_connect.write_data(write_df,index=0)
            kisti_connect.close()
            cateogory_item[cateogory_item["title_filter"].notnull()|cateogory_item["category_filter"].notnull()].to_csv("filter_out.csv",index=False)
        ratio = len(seed_temp)/seedtb_len
        logger.progress_writer(progress_container,ratio,f"seealso filter 중...({ratio*100:.1f}%)")
    logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : Item Filter 완료")

#ITEM MASTER 기준 SEEALSO FILTER TABLE 적재(UI 연결)
def filter_seealso(log_container,log_name,conf_name,max_cnt):
    conf=pf.set_config(conf_name)
    table_config=conf['table']['CRAWL_SEEALSO_FILTER_NETWORK']
    start_time=time.time()
    logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : Network Filter 시작")
    for n_cnt in range(1,max_cnt+1):
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : Network Filter 시작 - {n_cnt} 차수")
        kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
        if n_cnt==1: 
            kisti_connect.delete_record(index=0)
            seealso_tb=kisti_connect.read_query_data("*","where N_CNT=1 and from_id in (SELECT id from {0}) and to_id in (select id from {0})".format(table_config['source_table'][1]),index=0)
        else:
            if len(seealso_tb)>0:
                seealso_before=seealso_tb[["id","to_id"]]
                seealso_before.rename(columns={"to_id":"from_id"},inplace=True)
                seealso_tb=kisti_connect.read_query_data("*","where N_CNT={0} and from_id in (SELECT id from {1}) and to_id in (select id from {1})".format(n_cnt,table_config['source_table'][1]),index=0)
                if len(seealso_tb)>0:
                    seealso_tb = pd.merge(seealso_tb, seealso_before, left_on=("id","from_id"), right_on=("id","from_id"), how='inner')
        if len(seealso_tb)>0:
            kisti_connect.write_data(seealso_tb,index=0)
        kisti_connect.close()
        logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : Network Filter 종료 - {n_cnt} 차수")
    final_time=time.time()
    logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : Network Filter 종료 - 소요시간 : {final_time-start_time}")
    #new_item_master 생성
    logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : Final item master 시작")
    kisti_connect=data_connect.Data_connect(["DB","mysql"],conf,table_config=table_config)
    kisti_connect.delete_record(index=1)
    seealso_tb=kisti_connect.read_data(index=2).drop_duplicates()
    to_tb=seealso_tb[['TO_ID','TO_TITLE']].drop_duplicates()
    to_tb.columns=['ID','TITLE']
    from_tb=seealso_tb[['FROM_ID','FROM_TITLE']].drop_duplicates()
    from_tb.columns=['ID','TITLE']
    item_tb=pd.concat([to_tb,from_tb])
    item_tb=item_tb.drop_duplicates()
    kisti_connect.write_data(item_tb,index=1)
    kisti_connect.close()
    logger.log_writer(log_container,log_name,f"{datetime.datetime.now().strftime('%Y.%m.%d - %H:%M:%S')} : Final item master 종료")
    return 1