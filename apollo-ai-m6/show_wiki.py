import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import networkx as nx
import random #item_sub 임의로 지정하기 위해서 생성
import requests
import os

from data_connect import *
from paths import *
from preprocess_category import *

print('')

def do_scale(df) :
    min_max_scaler = MinMaxScaler(feature_range = (0, 100))
    column_list = ['PAGERANK', 'PAGEVIEWS', 'EPV']
    for column in column_list :
        df[column] = min_max_scaler.fit_transform(df[[column]])
    return df

#데이터 불러오기
conf_data=data_config
table_config=conf_data['table']['GET_API']

def load_data():
    kisti_connect=Data_connect(["DB","mysql"],conf_data,table_config=table_config)
    try:
        search_df=kisti_connect.read_data(index=0)
        item_df=kisti_connect.read_query_data("ID,TITLE,SECTION_TEXT,TRANSLATED,TECH_CLASS12,CATEGORY,SUB_CATEGORY","where ID in (select ID from wiki_item_info)",index=1)
        #item_df=kisti_connect.read_data(index=1)
        item_df['new_category']=item_df[['CATEGORY','TECH_CLASS12']].apply(replace_category,axis=1)
        item_df.rename(columns={"SECTION_TEXT":"SUMMARY","TRANSLATED":"SUMMARY_KOR","TECH_CLASS12":"TECH_CATE","new_category":"ITEM_CATE"},inplace=True)
        item_df['TITLE_KOR']=''
        del item_df['CATEGORY']
        #ID,TITLE,TITLE_KOR,SUMMARY,SUMMARY_KOR,TECH_CATE,ITEM_CATE
        #ID,TITLE,SECTION_TEXT,TRANSLATED,TECH_CLASS12,CATEGORY
        net_df=kisti_connect.read_query_data("distinct FROM_ID,TO_ID","",index=2)
        net_df.reset_index(drop=False, inplace=True)  # drop=False는 기존 인덱스를 열로 추가
        net_df.rename(columns={'index': 'rownum'}, inplace=True)  # 인덱스 열 이름 변경
        # ("PAGERANK":"기술집약도","PAGEVIEW":"수요부상성","EPV":"공급부상성")
        current_stat_df=kisti_connect.read_data(index=3).drop_duplicates()
        current_stat_df.columns=['ID','PAGERANK','PAGEVIEWS','EPV']
        current_stat_df['PAGERANK']=current_stat_df['PAGERANK'].astype(float)
        current_stat_df['PAGEVIEWS']=current_stat_df['PAGEVIEWS'].astype(float)
        current_stat_df['EPV']=current_stat_df['EPV'].astype(float)
        stats_df=kisti_connect.read_data(index=4)
        stats_df=stats_df[["ID","BASE_YEAR","NORM_PAGEVIEWS","NORM_EPV"]]
        stats_df.columns=["ID","YEAR","PAGEVIEWS","EPV"]
        stats_df['YEAR']=stats_df['YEAR'].astype(int)
        stats_df['PAGEVIEWS']=stats_df['PAGEVIEWS'].astype(float)
        stats_df['EPV']=stats_df['EPV'].astype(float)
    except Exception as e:
        print(e)
    finally:
        kisti_connect.close()
    return search_df,item_df,net_df,current_stat_df,stats_df

import time
start_time=time.time()
# ========================== data load ================================    
search_df,item_df,net_df,current_stat_df,stats_df= load_data()
# =====================================================================
print(f"소요시간 : {time.time()-start_time:.5f}")

def make_categorydict(df):
    from_df=df[["FROM_ID","차수"]].drop_duplicates()
    from_df.columns=["ID","차수"]
    to_df=df[["TO_ID","차수"]].drop_duplicates()
    to_df.columns=["ID","차수"]
    category_df=pd.concat([from_df,to_df]).drop_duplicates()
    category_dict=category_df["차수"].groupby(category_df["ID"]).min().to_dict()
    return category_dict

def select_layout(layout: str, g: nx) :
    
    layouts = {'spring': nx.spring_layout(g), 
               'spectral':nx.spectral_layout(g), 
               'shell':nx.shell_layout(g), 
               'circular':nx.circular_layout(g),
               'kamada_kawai':nx.kamada_kawai_layout(g), 
               'random':nx.random_layout(g)
              }
    
    return layouts[layout]
    
# 네트워크 확장 함수 -> (240430) 매 확장 마다 최대 확장 노드 10개로 제한, 최대 확장 노드의 수는 100~150개로 제한
def network_expand(item_list, net_df, before_list, n, node_out, indicator, n_out=0, max_expand_node=10):
    def change_col(row):
        if int(row['FROM_ID']) in item_list:
            to_id_temp=row['TO_ID']
            row['TO_ID']=row['FROM_ID']
            row['FROM_ID']=to_id_temp
        return row
    if n_out == n:
        return pd.DataFrame()
    tmp_df = net_df[(net_df['FROM_ID'].isin(item_list) | net_df['TO_ID'].isin(item_list)) & (~net_df['rownum'].isin(before_list))].copy()
    tmp_df = tmp_df.apply(lambda x: change_col(x), axis=1)
    tmp_df_group = tmp_df.groupby('TO_ID')['rownum'].count().reset_index(name='count')
    group_over, group_less=tmp_df_group[tmp_df_group['count']>max_expand_node]['TO_ID'], tmp_df_group[tmp_df_group['count']<=max_expand_node]['TO_ID']
    df = tmp_df[tmp_df['TO_ID'].isin(group_less)]
    for to_id in group_over:
        if indicator!="":
            df_over_id_tmp=pd.merge(tmp_df[tmp_df['TO_ID']==to_id],current_stat_df[["ID",indicator]],left_on="FROM_ID",right_on="ID",how="left")
            df_over_id=df_over_id_tmp.sort_values([indicator],ascending=False).reset_index(drop=True)[:max_expand_node] 
        else:
            df_over_id=tmp_df[tmp_df['TO_ID']==to_id].reset_index(drop=True)[:max_expand_node] #(241015)weight 기준 삭제
        df=pd.concat([df,df_over_id])
    df["차수"] =n_out + 1
    expanded_items = list(set(list(df['FROM_ID']) + list(df['TO_ID']))-set(item_list))
    left_node=node_out-len(before_list)
    tmp_list=list(set(list(df['rownum']))-set(before_list))
    if len(tmp_list)>left_node:
        df=df.reset_index(drop=True)[:left_node] #(241015)weight 기준 삭제
        return df
    else:
        before_list=list(set(before_list+list(df['rownum'])))
    return pd.concat([df, network_expand(expanded_items, net_df, before_list, n, node_out,indicator, n_out + 1)], ignore_index=True)


def depth_network(item_list, net_df, n, indicator, node_out):
    item_list=[int(search_df[search_df["REDIRECT"]==item]["ID"].drop_duplicates().iloc[0]) if isinstance(item, str) else item for item in item_list]
    final_net_df = network_expand(item_list, net_df, [], n ,node_out, indicator)
    final_net_df = final_net_df.sort_values(['차수']).reset_index(drop=True)
    final_net_df = pd.merge(final_net_df,item_df,left_on="FROM_ID",right_on="ID",how="inner")
    final_net_df = pd.merge(final_net_df,item_df,left_on="TO_ID",right_on="ID",how="inner")
    final_net_df = final_net_df.drop_duplicates()
    final_net_df = final_net_df.astype({"TO_ID":int,"FROM_ID":int})
    final_net_df.rename(columns={"TITLE_x":"TO_TITLE","TITLE_y":"FROM_TITLE"},inplace=True)
    final_net_df.drop(["ID_x","ID_y"],axis=1,inplace=True)
    return final_net_df

# 지표기반 네트워크 함수
def Indicators_network(Indicator, top_n):
    top_indicator=list(current_stat_df.sort_values(Indicator.upper(), ascending=False)['ID'])
    top_list=top_indicator[:top_n]
    return top_list

#지표기반 시각화
def graph_indicator_data(keyword: str, indicator:str = '', top_n:int = 0, n_cnt:int = 1):
    max_year=2024
    if len(search_df[search_df["REDIRECT"]==keyword]["ID"])>0 or len(search_df[search_df["ID"]==keyword]["ID"])>0:
        keyword_id=int(search_df[search_df["REDIRECT"]==keyword]["ID"].drop_duplicates().iloc[0]) if isinstance(keyword, str) else keyword
        keyword=int(search_df[search_df["ID"]==keyword]["TITLE"].drop_duplicates().iloc[0]) if isinstance(keyword, int) else keyword
    else:
        print('network가 없습니다.')
        return False, pd.DataFrame({"result" : "Not exsists."}, index=[keyword])
    if len(net_df[net_df['FROM_ID']==keyword_id])>0 or len(net_df[net_df['TO_ID']==keyword_id])>0:
        print('network가 존재합니다.')
        item_list=[keyword_id]
        final_net_df=depth_network(item_list, net_df, n_cnt, indicator, top_n)

        if len(final_net_df)>0:
            #=== 그래프 시각화 
            G=nx.DiGraph()
            edge_df=list(final_net_df[["FROM_TITLE","TO_TITLE"]].itertuples(index=False))
            edge_Key_df=list(final_net_df[["FROM_ID","TO_ID"]].itertuples(index=False))
            G.add_nodes_from(list(set(list(final_net_df["TO_TITLE"])+[keyword])))
            G.add_edges_from(list(edge_df))
            #=== 클러스터 지표 출력
            mask=current_stat_df['ID'].isin(list(set(list(final_net_df["TO_ID"])+list(final_net_df["FROM_ID"]))))
            final_df=current_stat_df.loc[mask,:].reset_index(drop=True)
            final_df=do_scale(final_df)
            category_dict=make_categorydict(final_net_df)
            final_df['node_value']=final_df[[indicator]]
            final_df=pd.merge(final_df,item_df[['ID','TITLE','TECH_CATE','ITEM_CATE','SUB_CATEGORY']],on="ID",how='left')
            final_df.rename(columns={'TECH_CATE':'tech_cate','ITEM_CATE':'item_cate'},inplace=True)
            #item_sub_list=['인명','지명','기관명','인증','규제','법','제도','기타'] #item_sub 임의로 지정하기 위해서 생성
            final_df['item_sub_cate']=final_df.apply(lambda x: x['SUB_CATEGORY'] if x['item_cate']=='비아이템' else '', axis=1) #item_sub 임의로 지정하기 위해서 생성
            final_df[["PAGERANK","PAGEVIEWS","EPV"]]=final_df[["PAGERANK","PAGEVIEWS","EPV"]].replace(0,0.01)
            key_name = final_df[['ID', 'TITLE', 'node_value','tech_cate','item_cate','item_sub_cate','PAGERANK','PAGEVIEWS','EPV']].to_dict() #item_sub 생성으로 인해 추가
            edges = edge_Key_df
            history_df=stats_df[(stats_df['ID']==keyword_id) & (stats_df['YEAR']>max_year-5)].drop_duplicates()
            history_df=history_df.fillna('')
            return True, key_name, edges, category_dict, final_df ,history_df
        else:
            print('생성할 network가 없습니다.')
            return False, pd.DataFrame({"result" : "Not make networks."}, index=[keyword])
    else:
        print('network가 없습니다.')
        return False, pd.DataFrame({"result" : "Not exsists."}, index=[keyword])

#네트워크 미리보기 시각화
def graph_preview_data(keyword: str, node_cnt: int = 30):
    if len(search_df[search_df["REDIRECT"]==keyword]["ID"])>0 or len(search_df[search_df["ID"]==keyword]["ID"])>0:
        keyword_id=int(search_df[search_df["REDIRECT"]==keyword]["ID"].drop_duplicates().iloc[0]) if isinstance(keyword, str) else keyword
        keyword=str(search_df[search_df["ID"]==keyword]["TITLE"].drop_duplicates().iloc[0]) if isinstance(keyword, int) else keyword
    else:
        print('network가 없습니다.')
        return False, pd.DataFrame({"result" : "Not exsists."}, index=[keyword])
    if len(net_df[net_df['FROM_ID']==keyword_id])>0 or len(net_df[net_df['TO_ID']==keyword_id])>0:
        print('network가 존재합니다.')
        item_list=[keyword_id]
        final_net_df=depth_network(item_list, net_df, 5, "" ,node_cnt) # 넷트워크 db 필터링
        #=== 그래프 시각화 
        if len(final_net_df)>0:   
            G=nx.DiGraph()
            edge_df=list(final_net_df[["FROM_TITLE","TO_TITLE"]].itertuples(index=False))
            edge_Key_df=list(final_net_df[["FROM_ID","TO_ID"]].itertuples(index=False))
            G.add_nodes_from(list(set(list(final_net_df["TO_TITLE"])+[keyword])))
            G.add_edges_from(list(edge_df))
            #=== 클러스터 지표 출력
            mask=current_stat_df['ID'].isin(list(set(list(final_net_df["TO_ID"])+list(final_net_df["FROM_ID"]))))
            final_df=current_stat_df.loc[mask,:].reset_index(drop=True)
            final_df=pd.merge(final_df,item_df[['ID','TITLE']],on="ID",how='left')
            final_df=do_scale(final_df)
            category_dict=make_categorydict(final_net_df)
            key_name = final_df[['ID', 'TITLE']].to_dict()

            edges = edge_Key_df

            return True, key_name, edges, category_dict, final_df
        else:
            print('생성할 network가 없습니다.')
            return False, pd.DataFrame({"result" : "Not make networks."}, index=[keyword])
    else:
        print('network가 없습니다.')
        return False, pd.DataFrame({"result" : "Not exsists."}, index=[keyword])

#  ITEM SEARCH 함수
def item_list_data(query_type, query, n_cnt):
    item_search_df=pd.DataFrame()
    url = os.getenv("SUGGEST_URL")  # 호출할 API 주소
    payload = {
        "keyword": query,
        "k":n_cnt,
        "query_type": query_type
    }
    response = requests.post(url, json=payload)
    response_list=response.json()
    print(response_list)
    item_search_df=pd.DataFrame()
    mask=(item_df['TITLE'].isin(response_list)) & (item_df['TITLE'].notna())
    item_search_df=item_df[mask]

    filtered_list = [title for title in response_list if title in item_search_df['TITLE'].values]
    item_search_df = item_search_df.set_index('TITLE').loc[filtered_list].reset_index()

    left_cnt=n_cnt-len(item_search_df)
    if len(item_search_df)>0:
        result_df=pd.merge(item_search_df,current_stat_df[["ID","PAGERANK","PAGEVIEWS","EPV"]].drop_duplicates(),on="ID",how="left")
        del result_df['SUB_CATEGORY']
        result_df=result_df[["ID","TITLE","TITLE_KOR","SUMMARY","SUMMARY_KOR","TECH_CATE","ITEM_CATE","PAGERANK","PAGEVIEWS","EPV"]]
        result_df[["PAGERANK","PAGEVIEWS","EPV"]]=result_df[["PAGERANK","PAGEVIEWS","EPV"]].replace(0,0.01)
        result_df=result_df.fillna('')
        return True, result_df
    else:
        print('network가 없습니다.')
        return False, pd.DataFrame({"result" : "Not exsists."}, index=[query])
